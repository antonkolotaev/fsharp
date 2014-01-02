module Make

   open System
   open System.IO
   open System.Reflection
   open Microsoft.Win32
   open Pervasives

   open DepsParser
   open DepsGraph
   open Transas.Areator.SceneBuilder
   open Transas.Areator.Utils.TextBox
      
   type IPipeline = 
      abstract member Generate   : ProgId * GeneratorArgs -> bool /// ProgId * Args -> Succeeded
      abstract member SaveDv     : ProgId * Filename -> bool  /// Dataview saved successfully? FileName can be null or ""
      abstract member LoadDv     : ProgId * Filename -> unit
      abstract member DeleteDv   : ProgId -> unit
      
   let formatArgs args = 
      let args = Map.toList args
      let f (x : string,y) = x + "=" + y
      if args.IsEmpty = false then 
         List.fold (fun acc x -> acc + "," + (f x)) (f args.Head) args.Tail
      else ""
      
   let getClsid = Utils.CLSIDFromProgID >> snd

   let getDllPath (clsid : Guid) = 
         let path = "SOFTWARE\\CLASSES\\CLSID\\" + clsid.ToString("B").ToUpper() + "\\InProcServer32"
         use key = Registry.LocalMachine.OpenSubKey(path)
         key.GetValue "" :?> string
      
   let timestamp_strict filename = 
      if File.Exists filename then File.GetLastWriteTime filename 
      else failwithf "File doesn't exist %s" filename
      
   let depsFolder() = (Assembly.GetCallingAssembly().Location |> Path.GetDirectoryName) .//. "Dp_Deps"

   let dvname workingDir name ext = workingDir .//. ext .//. (name + ".dv")

   let runPipeline (pipeline : IPipeline, workingDir, stages_to_do) = 

      let graph = DependencyGraph(Context(depsFolder()), "Areator.VisualExport")
      
      let getDllPath = memoize(getClsid >> getDllPath)
      let getTs = memoize timestamp_strict
      
      let dvname = dvname workingDir 
      let dvname' {name = name; ext = ext} = dvname name ext
      
      let progId_path x = x.name, x.ToString() 
      
      let saveDv failhandler x = x |> progId_path |> pipeline.SaveDv |> function false -> failhandler x | true -> ()
      
      let loadDv = progId_path >> pipeline.LoadDv
      
      let touchWithDLL x = 
         let dv_path = dvname' x 
         let dll_path = x.name |> getDllPath
         let dll_ts = dll_path |> getTs
         if timestamp_strict dv_path < dll_ts then 
            use logger = new LoggerStage("Touching dv " + dv_path + " with timestamp of changed DLL " + dll_path + ": " + dll_ts.ToString(), StageFlags.None)
            File.SetLastWriteTime(dv_path, dll_ts)

      if stages_to_do > Seq.length graph.Stages then false
      else
         
         let stages = graph.Stages |> if stages_to_do > 0 then Seq.take stages_to_do else id
         
         for g in stages do
         
            let copyDynDv { name = name; ext = ext } = 
               File.Copy(dvname name (g.PreviousExt name), dvname name ext, true) 
               File.SetLastWriteTime(dvname name ext, DateTime.Now)
                                                         
            let timestamp' def { name = name; ext = ext } = let path = dvname name ext
                                                            if File.Exists path then File.GetLastWriteTime path else def
                                                            
            let timestamp = timestamp' DateTime.MinValue                                                    
                                                            
            let generate() = 
               
               use logger = new LoggerStage("Processing " + g.ProgId, StageFlags.Temporary)
            
               logger.Warning(sprintf "%s : %A" g.ProgId g.Args) 
            
               let output_tss = g.OutputDataviews 
                                |> Seq.map (make_2_2 timestamp) |> List.ofSeq
               
               let output_ts = output_tss |> Seq.minBy snd 
                               
               if snd output_ts = DateTime.MinValue then 
                  logger.Info(dvname'(fst output_ts) + " doesn't exist --> regenerating")
                  true
               else
                  let fmt_dv x = dvname'(fst x) + ": " + (snd x).ToString()
                  
                  let generator_dll = g.ProgId |> getDllPath
                  let generator_ts  = generator_dll |> getTs
                  
                  let info = logger.Info
                  
                  if generator_ts > snd output_ts then 
                     info("Generator DLL '" + generator_dll + "' is older ("+ generator_ts.ToString() + 
                                    ") than the earliest output dv " + (fmt_dv output_ts))
                     true
                  else
                     let input_tss = Seq.concat [g.UsedDynamicDvs; g.UsedStaticDvs] 
                                     |> Seq.map (make_2_2 (dvname' >> timestamp_strict)) |> List.ofSeq
                                     
                     let input_ts = input_tss |> Seq.maxBy snd 
                     
                     info("the earliest output dv is " + (fmt_dv output_ts))
                     info("the latest input dv is " + (fmt_dv input_ts))
                     info(if snd input_ts > snd output_ts then " --> Regenerating!" else " --> leaving...")
                     
                     snd input_ts > snd output_ts
                                                                  
            if try_find "RunAlways" "" g.Args = "1" || generate() then
            
               if pipeline.Generate(g.ProgId, formatArgs g.Args) then 
                  Directory.CreateDirectory(workingDir .//. g.ProgId) |> ignore
                  g.CreatedDynamicDvs  |> Seq.iter (saveDv (failwithf "cannot save created dynamic dv %A"))
                  g.PossiblyModifiedDvs|> Seq.iter (saveDv copyDynDv)
                  g.CreatedStaticDvs   |> Seq.iter (saveDv (failwithf "cannot save created static dv %A"))
                  
            else 
            
               g.RemovedDvs         |> Seq.iter pipeline.DeleteDv
               g.OutputDataviews    |> Seq.iter loadDv
               g.OutputDataviews    |> Seq.iter touchWithDLL
               g.CreatedCompoundDvs |> Seq.iter (make_2_2 (constant null) >> pipeline.LoadDv)
         true