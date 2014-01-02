module DepsGraph

   open Pervasives
   open DepsParser
   
   type DependencyGraph(ctx : Context, target : ProgId) = 

      let created_by = lazy (ctx.Generators' 
                             |> Seq.collect (fun g -> (g.CreatedStaticDvs |~> (fun c -> c,g.ProgId)))
                             |> map)
                                
      let dyndv_deps = lazy (ctx.Dataviews
                               |>  Seq.collect (fun kv->snd(kv.Value).genorder |> Seq.pairwise) 
                               |>  Seq.groupBy snd                        
                               |~> map_2nd (Seq.map fst >> set)
                               |>  map)
                             
      let dependent_on = lazy (ctx.Generators' 
                               |~>  (fun g ->g.ProgId, 
                                             g.UsedStaticDvs 
                                               |> Seq.choose (!~created_by).TryFind 
                                               |> set 
                                               |> (+) (try_find g.ProgId Set.empty !~dyndv_deps))
                               |> map)
                               
      let build_order = lazy (topoSort !~dependent_on target)
      
      member x.CreatedBy dv = (!~created_by).TryFind dv
      member x.DependentOn = !~dependent_on
      member x.BuildOrder = !~build_order
      member x.Context = ctx
      
      member x.Stages = 
         let rec impl prev = function 
         | [] -> Seq.empty
         | head :: tail -> seq { let cur = PipelineStage(x, head, prev) in yield cur; yield! impl cur tail }
         let cur = PipelineStage(x, x.BuildOrder.Head)
         seq { yield cur; yield! impl cur x.BuildOrder.Tail } |> Seq.cache
                            

   and PipelineStage(graph : DependencyGraph, progId : ProgId, ?previous : PipelineStage) =

      let ctx = graph.Context
      let gen = ctx.Generator' progId
      
      let unp f dv = f dv progId
      let addRange = Seq.fold (flip (unp Map.add)) 

      let unp' f (x : DvExt) = f x.name x.ext
      let addRange' = Seq.fold (flip (unp' Map.add)) 
      
      let no = if previous.IsSome then previous.Value.No + 1 else 1
      
      let created_static_dvs = lazy (gen.CreatedStaticDvs 
                                     |.~> (fun x -> if x.ext <> "" && x.ext <> progId then failwith "xz" 
                                                    else {x with ext = progId} ))
      
      let prev_elaborations = lazy(match previous with Some p -> p.Elaborations | None -> Map.empty)
      
      let make_dvext s = { name = s; ext = "" }
      let elp x = {x with ext = try_find x.name "DataProcessingAdaptor" !~prev_elaborations}
      
      let used_static_dvs = lazy(gen.UsedStaticDvs |.~> elp)
      
      let elaborations = lazy (!~prev_elaborations 
                               |> flip addRange (gen.UsedDynamicDvs + gen.CreatedDynamicDvs) 
                               |> flip addRange' !~created_static_dvs)
      
      let used_dynamic_dvs = lazy (gen.UsedDynamicDvs |.~> (make_dvext >> elp))
      let created_dynamic_dvs = lazy (gen.CreatedDynamicDvs |.~> (fun s -> {name = s; ext = progId}))
      let possibly_modified_dvs = lazy ((gen.UsedDynamicDvs - gen.Removes) |.~> (fun s -> {name = s; ext = progId}))

      member x.ProgId = progId
      member x.Args = graph.Context.GeneratorArg progId
      
      member x.No = no
      
      member private x.Elaborations = !~elaborations
      
      member x.PreviousExt dv = try_find dv "" !~prev_elaborations
      
      member x.CreatedStaticDvs = !~created_static_dvs
      member x.UsedStaticDvs = !~used_static_dvs
      member x.RemovedDvs = gen.Removes
      
      member x.UsedDynamicDvs = !~used_dynamic_dvs
      member x.PossiblyModifiedDvs = !~possibly_modified_dvs
      member x.CreatedDynamicDvs = !~created_dynamic_dvs
      
      member x.CreatedCompoundDvs = gen.CreatedCompoundDvs
      
      member x.OutputDataviews = Seq.concat [x.CreatedDynamicDvs; x.CreatedStaticDvs; x.PossiblyModifiedDvs]
      
      override x.ToString() = progId
   
