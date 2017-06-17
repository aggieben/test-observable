open System
open System.IO
open System.Threading

open Microsoft.FSharp.Reflection
open Newtonsoft.Json
open Newtonsoft.Json.Linq

let GetUnionCaseName case =
    match FSharpValue.GetUnionFields(case, case.GetType()) with
    | case, _-> case.Name

type SubscribeRecord =
    { product_ids: string array }
    with 
        member this.GetJObject() =
            JObject(JProperty("product_ids", this.product_ids))

type HeartbeatCntlRecord =
    { on : bool }
    with
        member this.GetJObject() =
            JObject(JProperty("on", this.on))

type HeartbeatRecord =
    { sequence: int64;
      last_trade_id: int64;
      product_id: string;
      time: DateTime }
    with
        member this.GetJObject() =
            JObject(JProperty("sequence", this.sequence),
                    JProperty("last_trade_id", this.last_trade_id),
                    JProperty("product_id", this.product_id),
                    JProperty("time", this.time))

type ReceivedRecord =
    { time : DateTime
      product_id: string
      sequence: int64
      order_id: string
      funds: string
      side: string
      order_type: string }
    with
        member this.GetJObject() =
            JObject(JProperty("time", this.time),
                    JProperty("product_id", this.product_id),
                    JProperty("sequence", this.sequence),
                    JProperty("order_id", this.order_id),
                    JProperty("funds", this.funds),
                    JProperty("side", this.side),
                    JProperty("order_type", this.order_type))

type DoneRecord =
    { time: DateTime
      product_id: string
      sequence: int64
      price: decimal
      order_id: string
      reason: string
      side: string
      remaining_size: decimal }
    with
        member this.GetJObject() =
            JObject(JProperty("time", this.time),
                    JProperty("product_id", this.product_id),
                    JProperty("sequence", this.sequence),
                    JProperty("price", this.price),
                    JProperty("order_id", this.order_id),
                    JProperty("reason", this.reason),
                    JProperty("side", this.side),
                    JProperty("remaining_size", this.remaining_size))

type MatchRecord =
    { trade_id: int64
      sequence: int64
      maker_order_id: string
      taker_order_id: string
      time: DateTime
      product_id: string
      size: decimal
      price: decimal
      side: string }
    with 
        member this.GetJObject() =
            JObject(JProperty("trade_id", this.trade_id),
                    JProperty("sequence", this.sequence),
                    JProperty("maker_order_id", this.maker_order_id),
                    JProperty("taker_order_id", this.taker_order_id),
                    JProperty("time", this.time),
                    JProperty("product_id", this.product_id),
                    JProperty("size", this.size),
                    JProperty("price", this.price),
                    JProperty("side", this.side))

type GdaxFeedMessage =
    | Subscribe of SubscribeRecord
    | HeartbeatCntl of HeartbeatCntlRecord
    | Heartbeat of HeartbeatRecord
    | Received of ReceivedRecord
    | Done of DoneRecord
    | Match of MatchRecord

let deserializeMsg msg =
    let jo = JObject.Parse(msg)
    match string jo.["type"] with
    | "heartbeat" -> Heartbeat { sequence = int64 jo.["sequence"]
                                 last_trade_id = int64 jo.["last_trade_id"]
                                 product_id = string jo.["product_id"]
                                 time = DateTime.Parse(string jo.["time"]) }
                                 
    | "received" -> Received { time = DateTime.Parse(string jo.["time"])
                               product_id = string jo.["product_id"]
                               sequence = int64 jo.["sequence"]
                               order_id = string jo.["order_id"]
                               funds = string jo.["funds"]
                               side = string jo.["side"]
                               order_type = string jo.["order_type"] }

    | "done" -> Done { time = DateTime.Parse(string jo.["time"])
                       product_id = string jo.["product_id"]
                       sequence = int64 jo.["sequence"]
                       price = decimal jo.["price"]
                       order_id = string jo.["order_id"]
                       reason = string jo.["reason"]
                       side = string jo.["side"]
                       remaining_size = decimal jo.["remaining_size"]
                       }
                               
    | "match" -> Match { trade_id = int64 jo.["trade_id"]
                         sequence = int64 jo.["sequence"]
                         maker_order_id = string jo.["maker_order_id"]
                         taker_order_id = string jo.["taker_order_id"]
                         time = DateTime.Parse(string jo.["time"])
                         product_id = string jo.["product_id"]
                         size = decimal jo.["size"]
                         price = decimal jo.["price"]
                         side = string jo.["side"] }
                         
    | other -> failwith (sprintf "unrecognized message '%s'" other) 

let serializeMsg msg =
    let caseName = GetUnionCaseName msg
    let jo = JObject(JProperty("type", caseName.ToLowerInvariant()))
    match msg with
    | Subscribe (payload) -> jo.Merge(payload.GetJObject())
    | HeartbeatCntl (payload) -> jo.Merge(payload.GetJObject())
    | Heartbeat (payload) -> jo.Merge(payload.GetJObject())
    | Received (payload) -> jo.Merge(payload.GetJObject())
    | Done (payload) -> jo.Merge(payload.GetJObject())
    | Match (payload) -> jo.Merge(payload.GetJObject())
    jo.ToString(Newtonsoft.Json.Formatting.None)

let simulateFeed (simulation:string) =
    let simulationEvent = new Event<string>()

    let task = async {
        printfn "beginning simulation from %s" simulation

        use stream = File.OpenText(simulation)

        while not stream.EndOfStream do
            let! line = Async.AwaitTask <| stream.ReadLineAsync()

            simulationEvent.Trigger(line)

            do! Async.Sleep 100
    }

    (task, simulationEvent.Publish)

let isDone msg =
    printfn "isDone"
    match msg with
    | Done _ -> true
    | _ -> false

[<EntryPoint>]
let main argv =
    
    let feedProc, feedStream = simulateFeed argv.[0]

    let msgStream = feedStream |> Observable.map deserializeMsg

    msgStream 
    |> Observable.filter isDone
    |> Observable.subscribe (fun d -> printfn "%A" d)
    |> ignore

    Async.RunSynchronously feedProc

    0 // return an integer exit code
