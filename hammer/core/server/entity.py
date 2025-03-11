import grpc
from grpc import aio
from sqlalchemy import select

from ...config import CONF
from ...core.engine_utils import Postgres
from ..datahub.entity import Entity
from ..protos import entity_pb2, entity_pb2_grpc

db_config = CONF.infra["postgresql"]["hammer_meta"]
DB_NAME = "hammer"
db = Postgres(**db_config, database=DB_NAME)


class EntityService(entity_pb2_grpc.EntityServiceServicer):
    async def CreateEntity(self, request: entity_pb2.CreateEntityRequest, context) -> entity_pb2.CreateEntityResponse:
        async with db.get_db_session() as session:
            # 创建新实体
            new_entity = Entity(name=request.name, join_keys=list(request.join_keys))
            session.add(new_entity)
            await session.commit()
            await session.refresh(new_entity)

            # 返回响应
            return entity_pb2.CreateEntityResponse(
                id=new_entity.id, name=new_entity.name, join_keys=list(new_entity.join_keys)
            )

    async def GetEntity(self, request: entity_pb2.GetEntityRequest, context) -> entity_pb2.GetEntityResponse:
        async with db.get_db_session() as session:
            # 根据查询条件构建查询
            if request.HasField("id"):
                entity = await session.get(Entity, request.id)
            else:
                stmt = select(Entity).where(Entity.name == request.name)
                result = await session.execute(stmt)
                entity = result.scalar_one_or_none()

            # 如果未找到实体
            if entity is None:
                await context.abort(grpc.StatusCode.NOT_FOUND, "Entity not found")

            # 返回响应
            return entity_pb2.GetEntityResponse(
                entity=entity_pb2.Entity(id=entity.id, name=entity.name, join_keys=entity.join_keys)
            )


async def entity_serve():
    server = aio.server()
    entity_pb2_grpc.add_EntityServiceServicer_to_server(EntityService(), server)
    server.add_insecure_port("[::]:50051")
    await server.start()
    await server.wait_for_termination()
