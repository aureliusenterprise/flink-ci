from collections.abc import Callable

from m4i_atlas_core import EntityAuditAction

from flink_tasks.determine_change.model import AtlasChangeMessageWithPreviousVersion, EntityMessage

from .handle_create_operation import handle_create_operation
from .handle_delete_operation import handle_delete_operation
from .handle_update_operation import handle_update_operation

ChangeEventHandler = Callable[[AtlasChangeMessageWithPreviousVersion], list[EntityMessage]]

EVENT_HANDLERS: dict[EntityAuditAction, ChangeEventHandler] = {
    EntityAuditAction.ENTITY_CREATE: handle_create_operation,
    EntityAuditAction.ENTITY_UPDATE: handle_update_operation,
    EntityAuditAction.ENTITY_DELETE: handle_delete_operation,
}
