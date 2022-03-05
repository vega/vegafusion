import grpcWeb, {GrpcWebClientBase} from "grpc-web"
import {MsgReceiver} from "./index";

// Other utility functions
export function makeGrpcSendMessageFn(client: GrpcWebClientBase, hostname: string) {
    let sendMessageGrpc = (send_msg_bytes: Uint8Array, receiver: MsgReceiver) => {
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

        let promise = client.thenableCall(
            hostname + grpc_route,
            send_msg_bytes,
            {},
            methodDescriptor,
        );
        promise.then((response: any) => {
            receiver.receive(response)
        })
    }
    return sendMessageGrpc
}
