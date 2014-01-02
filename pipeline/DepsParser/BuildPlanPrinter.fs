module BuildPlanPrinter

   open Pervasives
   open DepsGraph
   open DepsParser
   open System.IO
   
   let makeKVs (x : string,y) = x + "=" + y
   
   let formatArgs args = 
      let args = Map.toList args
      let f (x : string,y) = x + "=" + y
      if args.IsEmpty = false then 
         List.fold (fun acc x -> acc + "," + (f x)) (f args.Head) args.Tail
      else ""
   
   
   let buildPlanGenerator ctx target output_filename inputTs = 
   
     
      printf "makefile for %s" target
      
      let runalways = false 
      
      if runalways || not(File.Exists output_filename) || inputTs > (File.GetLastWriteTime output_filename) then 
      
         let graph = DependencyGraph(ctx, target)
         
         let tab_1 = (+) "   "
         let tab_2 = (+) "      "
         
         let localFormat { name = name; ext = ext } = ext + @"\" + name + ".dv"
         
         let sq x = seq { yield x }
         let tab_1 = tab_1 >> sq
         
         let sec name elems = if Seq.isEmpty elems then Seq.empty else Seq.concat [tab_1 name; elems |~> tab_2]
         let name { name = name; ext = _ } = name
         
         let stageDescription (g : PipelineStage) = 
         
            let input_dvs = g.UsedDynamicDvs +++ g.UsedStaticDvs
         
            Seq.concat 
               [
                  sq g.ProgId
                  sec "Arguments:"          (g.Args |> demap      |~> makeKVs)
                  sec "CreatedStaticDvs:"   (g.CreatedStaticDvs   |~> name) 
                  sec "CreatedDynamicDvs:"  (g.CreatedDynamicDvs  |~> name) 
                  sec "PossiblyModifiedDvs:"(g.PossiblyModifiedDvs|~> name)
                  sec "InputDvs:"           (input_dvs            |~> localFormat) 
                  sec "RemovedDvs:"          g.RemovedDvs         
                  sec "CreatedCompoundDvs:"  g.CreatedCompoundDvs 
                  sq "end."
               ]
         
         
         graph.Stages 
            |~> stageDescription 
            |> Seq.concat 
            |> Seq.toArray 
            |> pairify File.WriteAllLines output_filename
         
         printf "...ok!\n"
         
      else printf ".\n"   

