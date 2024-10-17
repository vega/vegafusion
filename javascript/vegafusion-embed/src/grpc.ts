import grpcWeb, {GrpcWebClientBase} from "grpc-web"

// Other utility functions
export function makeGrpcSendMessageFn(client: GrpcWebClientBase, hostname: string) {
    let sendMessageGrpc = async (send_msg_bytes: Uint8Array) => {
        let grpc_route = '/services.VegaFusionRuntime/TaskGraphQuery'

        // Make custom MethodDescriptor that does not perform serialization
        const methodDescriptor = new grpcWeb.MethodDescriptor(
            grpc_route,
            grpcWeb.MethodType.UNARY,
            Uint8Array as any,
            Uint8Array as any,
            (v: any) => v,
            (v: any) => v,
        );

        return await client.thenableCall(
            hostname + grpc_route,
            send_msg_bytes,
            {},
            methodDescriptor,
        );
    }
    return sendMessageGrpc
}
