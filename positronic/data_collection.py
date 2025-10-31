import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import nullcontext
from enum import Enum
from pathlib import Path

import configuronic as cfn
import numpy as np

import pimm
import positronic.cfg.hardware.camera
import positronic.cfg.hardware.gripper
import positronic.cfg.hardware.roboarm
import positronic.cfg.simulator
import positronic.cfg.sound
import positronic.cfg.webxr
from positronic import geom, wire
from positronic.dataset.ds_writer_agent import (
    DsWriterAgent,
    DsWriterCommand,
    DsWriterCommandType,
    Serializers,
    TimeMode,
)
from positronic.dataset.local_dataset import LocalDatasetWriter
from positronic.drivers import roboarm
from positronic.drivers.webxr import WebXR
from positronic.gui.dpg import DearpyguiUi
from positronic.simulator.mujoco.sim import MujocoCameras, MujocoFranka, MujocoGripper, MujocoSim
from positronic.simulator.mujoco.transforms import MujocoSceneTransform
from positronic.utils.buttons import ButtonHandler


def _parse_buttons(buttons: dict, button_handler: ButtonHandler):
    for side in ['left', 'right']:
        if buttons[side] is None:
            continue

        mapping = {
            f'{side}_A': buttons[side][4],
            f'{side}_B': buttons[side][5],
            f'{side}_trigger': buttons[side][0],
            f'{side}_thumb': buttons[side][1],
            f'{side}_stick': buttons[side][3],
        }
        button_handler.update_buttons(mapping)


class _Tracker:
    on = False
    _offset = geom.Transform3D()
    _teleop_t = geom.Transform3D()

    def __init__(self, operator_position: geom.Transform3D | None):
        self._operator_position = operator_position
        self.on = self.umi_mode

    @property
    def umi_mode(self):
        return self._operator_position is None

    def turn_on(self, robot_pos: geom.Transform3D):
        if self.umi_mode:
            print('Ignoring tracking on/off in UMI mode')
            return

        self.on = True
        print('Starting tracking')
        self._offset = geom.Transform3D(
            -self._teleop_t.translation + robot_pos.translation, self._teleop_t.rotation.inv * robot_pos.rotation
        )

    def turn_off(self):
        if self.umi_mode:
            print('Ignoring tracking on/off in UMI mode')
            return
        self.on = False
        print('Stopped tracking')

    def update(self, tracker_pos: geom.Transform3D):
        if self.umi_mode:
            return tracker_pos

        self._teleop_t = self._operator_position * tracker_pos * self._operator_position.inv
        return geom.Transform3D(
            self._teleop_t.translation + self._offset.translation, self._teleop_t.rotation * self._offset.rotation
        )


class OperatorPosition(Enum):
    # map xyz -> zxy
    FRONT = geom.Transform3D(rotation=geom.Rotation.from_quat([0.5, 0.5, 0.5, 0.5]))
    # map xyz -> zxy + flip x and y
    BACK = geom.Transform3D(rotation=geom.Rotation.from_quat([-0.5, -0.5, 0.5, 0.5]))


class DataCollectionController(pimm.ControlSystem):
    def __init__(self, operator_position: geom.Transform3D | None, metadata_getter: Callable[[], dict] | None = None):
        self.operator_position = operator_position
        self.metadata_getter = metadata_getter or (lambda: {})
        self.controller_positions = pimm.ControlSystemReceiver(self, default=None)
        self.buttons_receiver = pimm.ControlSystemReceiver(self)
        self.robot_state = pimm.ControlSystemReceiver(self)
        self.gripper_state = pimm.FakeReceiver(self)  # To make compatible with other "policy" control systems
        self.frames = pimm.ReceiverDict(self, fake=True)

        self.robot_commands = pimm.ControlSystemEmitter(self)
        self.target_grip = pimm.ControlSystemEmitter(self)

        self.ds_agent_commands = pimm.ControlSystemEmitter(self)
        self.sound = pimm.ControlSystemEmitter(self)

    def run(self, should_stop: pimm.SignalReceiver, clock: pimm.Clock) -> Iterator[pimm.Sleep]:  # noqa: C901
        start_wav_path = 'positronic/assets/sounds/recording-has-started.wav'
        end_wav_path = 'positronic/assets/sounds/recording-has-stopped.wav'
        abort_wav_path = 'positronic/assets/sounds/recording-has-been-aborted.wav'

        tracker = _Tracker(self.operator_position)
        button_handler = ButtonHandler()

        recording = False

        while not should_stop.value:
            try:
                _parse_buttons(self.buttons_receiver.value, button_handler)
                if button_handler.just_pressed('right_B'):
                    op = DsWriterCommandType.START_EPISODE if not recording else DsWriterCommandType.STOP_EPISODE
                    meta = self.metadata_getter() if op == DsWriterCommandType.START_EPISODE else {}
                    self.ds_agent_commands.emit(DsWriterCommand(op, meta))
                    self.sound.emit(start_wav_path if not recording else end_wav_path)
                    recording = not recording
                elif button_handler.just_pressed('right_A'):
                    if tracker.on:
                        tracker.turn_off()
                    else:
                        tracker.turn_on(self.robot_state.value.ee_pose)
                elif button_handler.just_pressed('right_stick') and not tracker.umi_mode:
                    print('Resetting robot')
                    if recording:
                        self.ds_agent_commands.emit(DsWriterCommand.ABORT())
                        self.sound.emit(abort_wav_path)
                    tracker.turn_off()
                    recording = False
                    self.robot_commands.emit(roboarm.command.Reset())

                self.target_grip.emit(button_handler.get_value('right_trigger'))
                cp_msg = self.controller_positions.read()
                if cp_msg.updated:
                    target_robot_pos = tracker.update(cp_msg.data['right'])
                    if tracker.on:  # Don't spam the robot with commands.
                        self.robot_commands.emit(roboarm.command.CartesianPosition(target_robot_pos))

                yield pimm.Sleep(0.001)

            except pimm.NoValueException:
                yield pimm.Sleep(0.001)
                continue


def controller_positions_serializer(controller_positions: dict[str, geom.Transform3D]) -> dict[str, np.ndarray]:
    res = {}
    for side, pos in controller_positions.items():
        if pos is not None:
            res[f'.{side}'] = Serializers.transform_3d(pos)
    return res


def _wrench_to_level(state: roboarm.State) -> float | None:
    if state.ee_wrench is None:
        return None
    return np.linalg.norm(state.ee_wrench)


def _wire(
    world: pimm.World,
    ds_agent: DsWriterAgent | None,
    data_collection: DataCollectionController,
    webxr: WebXR,
    robot_arm: pimm.ControlSystem | None,
    sound: pimm.ControlSystem | None,
):
    world.connect(webxr.controller_positions, data_collection.controller_positions)
    world.connect(webxr.buttons, data_collection.buttons_receiver)

    if sound is not None:
        world.connect(data_collection.sound, sound.wav_path)
        world.connect(robot_arm.state, sound.level, emitter_wrapper=pimm.map(_wrench_to_level))

    if ds_agent is not None:
        ds_agent.add_signal('controller_positions', controller_positions_serializer)
        world.connect(webxr.controller_positions, ds_agent.inputs['controller_positions'])
        world.connect(data_collection.ds_agent_commands, ds_agent.command)

    return ds_agent


def main(
    robot_arm: pimm.ControlSystem | None,
    gripper: pimm.ControlSystem | None,
    webxr: WebXR,
    sound: pimm.ControlSystem | None,
    cameras: dict[str, pimm.ControlSystem] | None,
    output_dir: str | None = None,
    stream_video_to_webxr: str | None = None,
    operator_position: OperatorPosition = OperatorPosition.FRONT,
    task: str | None = None,
):
    """Runs data collection in real hardware."""
    # Convert camera instances to emitters for wire()
    camera_instances = cameras or {}
    camera_emitters = {name: cam.frame for name, cam in camera_instances.items()}
    static_getter = None if task is None else lambda: {'task': task}
    data_collection = DataCollectionController(operator_position.value, metadata_getter=static_getter)

    writer_cm = LocalDatasetWriter(Path(output_dir)) if output_dir is not None else nullcontext(None)
    with writer_cm as dataset_writer, pimm.World() as world:
        ds_agent = wire.wire(world, data_collection, dataset_writer, camera_emitters, robot_arm, gripper, None)
        _wire(world, ds_agent, data_collection, webxr, robot_arm, sound)

        bg_cs = [webxr, *camera_instances.values(), ds_agent, robot_arm, gripper, sound]

        if stream_video_to_webxr is not None:
            world.connect(
                camera_emitters[stream_video_to_webxr],
                webxr.frame,
                receiver_wrapper=pimm.map(lambda adapter: adapter.array),
            )

        dc_steps = iter(world.start(data_collection, bg_cs))
        while not world.should_stop:
            try:
                time.sleep(next(dc_steps).seconds)
            except StopIteration:
                break


@cfn.config(
    mujoco_model_path='positronic/assets/mujoco/franka_table.xml',
    webxr=positronic.cfg.webxr.oculus,
    cameras={
        'image.handcam_left': 'handcam_left_ph',
        'image.handcam_right': 'handcam_right_ph',
        'image.back_view': 'back_view_ph',
        'image.wrist': 'wrist_cam_ph',
    },
    sound=positronic.cfg.sound.sound,
    operator_position=OperatorPosition.BACK,
    loaders=positronic.cfg.simulator.stack_cubes_loaders,
)
def main_sim(
    mujoco_model_path: str,
    webxr: WebXR,
    cameras: dict[str, str],
    sound: pimm.ControlSystem | None = None,
    loaders: Sequence[MujocoSceneTransform] = (),
    output_dir: str | None = None,
    fps: int = 30,
    operator_position: OperatorPosition = OperatorPosition.FRONT,
    task: str | None = None,
):
    """Runs data collection in simulator."""

    sim = MujocoSim(mujoco_model_path, loaders)
    robot_arm = MujocoFranka(sim, suffix='_ph')

    mujoco_cameras = MujocoCameras(sim.model, sim.data, resolution=(320, 240), fps=fps)
    cameras = {name: mujoco_cameras.cameras[orig_name] for name, orig_name in cameras.items()}
    gui = DearpyguiUi()
    gripper = MujocoGripper(sim, actuator_name='actuator8_ph', joint_name='finger_joint1_ph')

    def metadata_getter():
        result = {k: v.tolist() for k, v in sim.save_state().items()}
        if task is not None:
            result['task'] = task
        return result

    data_collection = DataCollectionController(operator_position.value, metadata_getter=metadata_getter)

    writer_cm = LocalDatasetWriter(Path(output_dir)) if output_dir is not None else nullcontext(None)
    with writer_cm as dataset_writer, pimm.World(clock=sim) as world:
        ds_agent = wire.wire(world, data_collection, dataset_writer, cameras, robot_arm, gripper, gui, TimeMode.MESSAGE)

        _wire(world, ds_agent, data_collection, webxr, robot_arm, sound)

        sim_iter = world.start(
            [sim, mujoco_cameras, robot_arm, gripper, data_collection], [webxr, gui, ds_agent, sound]
        )
        sim_iter = iter(sim_iter)

        start_time = pimm.world.SystemClock().now_ns()
        sim_start_time = sim.now_ns()

        while not world.should_stop:
            try:
                time_since_start = pimm.world.SystemClock().now_ns() - start_time
                if sim.now_ns() < sim_start_time + time_since_start:
                    next(sim_iter)
                else:
                    time.sleep(0.001)
            except StopIteration:
                break


main_cfg = cfn.Config(
    main,
    robot_arm=None,
    gripper=positronic.cfg.hardware.gripper.dh_gripper,
    webxr=positronic.cfg.webxr.oculus,
    sound=positronic.cfg.sound.sound,
    cameras={
        'image.left': positronic.cfg.hardware.camera.arducam_left,
        'image.right': positronic.cfg.hardware.camera.arducam_right,
    },
    operator_position=OperatorPosition.FRONT,
)


@cfn.config(
    robot_arm=positronic.cfg.hardware.roboarm.so101,
    webxr=positronic.cfg.webxr.oculus,
    sound=positronic.cfg.sound.sound,
    operator_position=OperatorPosition.BACK,
    cameras={'image.right': positronic.cfg.hardware.camera.arducam_right},
)
def so101cfg(robot_arm, **kwargs):
    """Runs data collection on SO101 robot"""
    main(robot_arm=robot_arm, gripper=robot_arm, **kwargs)


droid = cfn.Config(
    main,
    robot_arm=positronic.cfg.hardware.roboarm.franka_droid,
    gripper=positronic.cfg.hardware.gripper.robotiq,
    webxr=positronic.cfg.webxr.oculus,
    sound=positronic.cfg.sound.sound,
    cameras={
        'image.wrist': positronic.cfg.hardware.camera.zed_m.override(view='left', resolution='hd720', fps=30),
        'image.exterior': positronic.cfg.hardware.camera.zed_2i.override(view='left', resolution='hd720', fps=30),
    },
    operator_position=OperatorPosition.BACK,
)


def _internal_main():
    cfn.cli({'real': main_cfg, 'so101': so101cfg, 'sim': main_sim, 'droid': droid})


if __name__ == '__main__':
    _internal_main()
