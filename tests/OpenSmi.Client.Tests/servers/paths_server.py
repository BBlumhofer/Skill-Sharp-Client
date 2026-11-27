# Port of opensmi-client tests/integration/conftest.py (9026539) as a standalone server for PathTests.
#
# Usage: python paths_server.py <port> [legacy]
#
# Default: machine M1 with one object of every OpenSMI kind (nested component, resource, methods, a skill with
# feasibility and precondition check, a variable shared by two groups, lock, users) plus raw asyncua variables with
# names opensmi-server would never produce ("/", empty, the same name in another namespace) and a raw variable
# referenced by two groups with HasComponent; machine M2 without own objects.
# legacy: M1 and M2 without own objects, users moved from Objects/Users below each machine's
# MachineryBuildingBlocks/Users (the layout before opensmi-server 0.3).

import asyncio
import sys

from asyncua import ua
from opensmi.core.nodesets import SmartFactoryMachineSetNodeIds
from opensmi.core.ua_node_util import get_child_without_ns
from opensmi.server import (
    BaseComponent,
    BaseMachine,
    BaseMachineryItem,
    BaseMethod,
    BaseSkillFinite,
    Monitoring,
    ParameterSet,
    Server,
    UaVariable,
)
from opensmi.server.base_feasibility_check import BaseFeasibilityCheck
from opensmi.server.base_precondition_check import BasePreconditionCheck
from opensmi.server.config import ServerConfiguration
from opensmi.server.mixins import (
    MonitoringMixin,
    NotificationForwarderMixin,
    ParameterSetMixin,
    ParentMixin,
    ResourcesMixin,
)
from opensmi.server.resource import ResourceDefinition

FOREIGN_NAMESPACE = "urn:opensmi-client:tests:foreign"


class _SkillParameterSet(ParameterSet):
    Speed = UaVariable(1.0)


class _Skill(ParameterSetMixin[_SkillParameterSet], BaseSkillFinite):
    async def _handle_running(self) -> None:
        pass


class _FeasibilityCheck(BaseFeasibilityCheck):
    async def _handle_running(self) -> None:
        pass


class _PreconditionCheck(BasePreconditionCheck):
    async def _handle_running(self) -> None:
        pass


class _Method(ParentMixin[BaseMachineryItem], BaseMethod):
    async def execute_method(self) -> None:
        pass


class _Component(ParentMixin[BaseMachineryItem], NotificationForwarderMixin, BaseComponent):
    async def _write_identification(self) -> None:
        await self.identification.SerialNumber.write("component-1")
        await self.identification.Manufacturer.write(ua.LocalizedText("SmartFactory KL", "en"))

    async def _init(self) -> None:
        await super()._init()
        await self.add_skill(_Skill(name="Grip"))
        if isinstance(self.parent, BaseMachine):
            await self.add_component(_Component(name="Finger"))


class _MachineParameterSet(ParameterSet):
    Force = UaVariable(1.0)


class _MachineMonitoring(Monitoring):
    Temperature = UaVariable(20.0)


class TestMachine(
    MonitoringMixin[_MachineMonitoring],
    ParameterSetMixin[_MachineParameterSet],
    ResourcesMixin,
    BaseMachine,
):
    async def _write_identification(self) -> None:
        await self.identification.SerialNumber.write("machine-1")
        await self.identification.ProductInstanceUri.write("urn:opensmi-client:tests:machine-1")
        await self.identification.Manufacturer.write(ua.LocalizedText("SmartFactory KL", "en"))

    async def _init(self) -> None:
        await super()._init()
        skill = await self.add_skill(_Skill(name="Pick"))
        await skill.add_feasibility_check(_FeasibilityCheck())
        await skill.add_precondition_check(_PreconditionCheck())
        await self.add_method(_Method(name="Home"))
        await self.add_method(_Method(name="Pick"))  # same name as a skill
        await self.add_component(_Component(name="Gripper"))
        await self.resources.add(ResourceDefinition(asset_id="snr-1", component_name="Battery", resource_class="Pack"))
        # a variable listed in two groups: owned by ParameterSet, also in Monitoring
        await self.monitoring.add(self.parameter_set.Force)


class _PlainMachine(BaseMachine):
    def __init__(self, name: str, serial: str) -> None:
        super().__init__(name=name)
        self._serial = serial

    async def _write_identification(self) -> None:
        await self.identification.SerialNumber.write(self._serial)
        await self.identification.ProductInstanceUri.write(f"urn:opensmi-client:tests:{self._serial}")
        await self.identification.Manufacturer.write(ua.LocalizedText("SmartFactory KL", "en"))


async def _add_raw_variables(server: Server, machine: TestMachine) -> None:
    monitoring = machine.monitoring.ua_node
    foreign = await server.ua_server.register_namespace(FOREIGN_NAMESPACE)
    own = server.ua_get_namespace_index(SmartFactoryMachineSetNodeIds.URI)
    await monitoring.add_variable(ua.NodeId(ua.String("raw.slash"), own), ua.QualifiedName("Pick/Place", own), 1.0)
    await monitoring.add_variable(ua.NodeId(ua.String("raw.empty"), own), ua.QualifiedName("", own), 2.0)
    await monitoring.add_variable(
        ua.NodeId(ua.String("raw.duplicate"), ua.Int16(foreign)), ua.QualifiedName("Temperature", foreign), 3.0
    )
    shared = await machine.parameter_set.ua_node.add_variable(
        ua.NodeId(ua.String("raw.shared"), own), ua.QualifiedName("Shared", own), 4.0
    )
    await monitoring.add_reference(shared.nodeid, ua.object_ids.ObjectIds.HasComponent)


async def _move_users_to_the_machines(server: Server) -> None:
    users = [user.ua_node for user in server.users]
    await server.users.ua_node.delete(recursive=False)
    for machine in server.machines:
        building_blocks = await get_child_without_ns(machine.ua_node, browse_name="MachineryBuildingBlocks")
        folder = await building_blocks.add_object(
            ua.NodeId(ua.String(f"legacy.{machine.name}.Users"), ua.Int16(1)),
            ua.QualifiedName("Users", server.ua_get_namespace_index(SmartFactoryMachineSetNodeIds.URI)),
            objecttype=server.ua_get_node_id(SmartFactoryMachineSetNodeIds.UsersType),
            instantiate_optional=False,
        )
        for user in users:
            await folder.add_reference(user.nodeid, ua.object_ids.ObjectIds.HasComponent)


async def main(port: int, legacy: bool) -> None:
    config = ServerConfiguration()
    config.ua_server.endpoint_address = f"opc.tcp://127.0.0.1:{port}/server"
    async with Server(config=config) as server:
        if legacy:
            await server.add_machine(_PlainMachine("M1", "machine-1"))
            await server.add_machine(_PlainMachine("M2", "machine-2"))
            await _move_users_to_the_machines(server)
        else:
            machine = TestMachine(name="M1")
            await server.add_machine(machine)
            await server.add_machine(_PlainMachine("M2", "machine-2"))
            await _add_raw_variables(server, machine)
        await server.start(blocking=True)


if __name__ == "__main__":
    asyncio.run(main(int(sys.argv[1]), len(sys.argv) > 2 and sys.argv[2] == "legacy"))
