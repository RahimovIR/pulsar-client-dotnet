namespace Pulsar.Client.Internal

open System
open Microsoft.Extensions.Logging
open Pulsar.Client.Api
open Pulsar.Client.Common

type ConsumerInterceptors(interceptor: IConsumerInterceptor list) =

     let exceptionPreocess result funAction =
          let tryWrapFun exn inter =
               try
                    funAction exn inter
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor callback ", e)
                    ()
          
          match result with
          | Result.Ok _ -> interceptor |> List.iter (tryWrapFun null)
          | Result.Error exn -> interceptor |> List.iter (tryWrapFun exn)
     member this.Interceptors = interceptor
     
     static member Empty with get() = ConsumerInterceptors([])
     
     member this.BeforeConsume (consumer: IConsumer) (msg: Message) =
          let funAction (acc:Message) (inter:IConsumerInterceptor) =
               try
                    inter.BeforeConsume(consumer, acc)
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor beforeConsume callback ", e)
                    acc
             
          interceptor |> List.fold funAction msg
        
     member this.OnAcknowledge (consumer: IConsumer)(msgId: MessageId) (result:Result<_,Exception>) =
          let funAction exn (inter:IConsumerInterceptor) =
               inter.OnAcknowledge(consumer, msgId, exn)
               
          exceptionPreocess result funAction
          result

     member this.OnAcknowledgeCumulative (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          let funAction exn (inter:IConsumerInterceptor) =
               inter.OnAcknowledgeCumulative(consumer, msgId, exn)
               
          exceptionPreocess result funAction
          result

     member this.OnNegativeAcksSend (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          let funAction exn (inter:IConsumerInterceptor) =
               inter.OnNegativeAcksSend(consumer, msgId, exn)
               
          exceptionPreocess result funAction
          result
          
     member this.OnAckTimeoutSend (consumer: IConsumer) (msgId: MessageId) (result:Result<_,Exception>) =
          let funAction exn (inter:IConsumerInterceptor) =
               inter.OnAckTimeoutSend(consumer, msgId, exn)
               
          exceptionPreocess result funAction
          result
          
     member this.Close() =
          let funAction (inter:IConsumerInterceptor) =
               try
                    inter.Close()
               with e ->
                    Log.Logger.LogWarning("Error executing interceptor close callback ", e)
                    ()
          interceptor |> List.iter funAction