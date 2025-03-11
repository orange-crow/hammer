# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""

import grpc

from . import entity_pb2 as entity__pb2

GRPC_GENERATED_VERSION = "1.71.0"
GRPC_VERSION = grpc.__version__
_version_not_supported = False

try:
    from grpc._utilities import first_version_is_lower

    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True

if _version_not_supported:
    raise RuntimeError(
        f"The grpc package installed is at version {GRPC_VERSION},"
        + " but the generated code in entity_pb2_grpc.py depends on"
        + f" grpcio>={GRPC_GENERATED_VERSION}."
        + f" Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}"
        + f" or downgrade your generated code using grpcio-tools<={GRPC_VERSION}."
    )


class EntityServiceStub(object):
    """Entity 服务定义"""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.CreateEntity = channel.unary_unary(
            "/entity.EntityService/CreateEntity",
            request_serializer=entity__pb2.CreateEntityRequest.SerializeToString,
            response_deserializer=entity__pb2.CreateEntityResponse.FromString,
            _registered_method=True,
        )
        self.GetEntity = channel.unary_unary(
            "/entity.EntityService/GetEntity",
            request_serializer=entity__pb2.GetEntityRequest.SerializeToString,
            response_deserializer=entity__pb2.GetEntityResponse.FromString,
            _registered_method=True,
        )


class EntityServiceServicer(object):
    """Entity 服务定义"""

    def CreateEntity(self, request, context):
        """创建新 Entity"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetEntity(self, request, context):
        """查询 Entity"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_EntityServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "CreateEntity": grpc.unary_unary_rpc_method_handler(
            servicer.CreateEntity,
            request_deserializer=entity__pb2.CreateEntityRequest.FromString,
            response_serializer=entity__pb2.CreateEntityResponse.SerializeToString,
        ),
        "GetEntity": grpc.unary_unary_rpc_method_handler(
            servicer.GetEntity,
            request_deserializer=entity__pb2.GetEntityRequest.FromString,
            response_serializer=entity__pb2.GetEntityResponse.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler("entity.EntityService", rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers("entity.EntityService", rpc_method_handlers)


# This class is part of an EXPERIMENTAL API.
class EntityService(object):
    """Entity 服务定义"""

    @staticmethod
    def CreateEntity(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/entity.EntityService/CreateEntity",
            entity__pb2.CreateEntityRequest.SerializeToString,
            entity__pb2.CreateEntityResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True,
        )

    @staticmethod
    def GetEntity(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/entity.EntityService/GetEntity",
            entity__pb2.GetEntityRequest.SerializeToString,
            entity__pb2.GetEntityResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
            _registered_method=True,
        )
