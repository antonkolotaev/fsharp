module DepsParser 

   open System
   open System.IO
   open System.Collections.Generic
   open Pervasives

   type ProgId = string
   type CLSID = string
   type GeneratorArgs = string
   type Filename = string

   let wrap f x = try f x with e -> raise (new Exception("While processing " + x.ToString(), e))

   let list = List.ofSeq
    
   type CycleInGraph<'a> (cycle : 'a list) = 
      inherit System.Exception()
      member x.Cycle = cycle
      override x.Message = sprintf "a cycle in graph detected: %A" cycle

   let contains xs x = Seq.exists ((=) x) xs

   let rec headTo e = function 
      |  x :: _ when x = e -> [e]
      |  x :: xs -> x :: (headTo e xs)
      |  [] -> []

   let topoSort (graph : Map<_,_>) target = 

      let rec dfs grey result v = 
      
         let grey = v :: grey 
         
         let result' = 
           Seq.fold (fun result e -> 
                        if contains result e then result 
                        elif contains grey e then CycleInGraph(headTo e grey |> List.rev) |> raise
                        else dfs grey result e) result graph.[v]
         
         v :: result'

      target |> dfs [] [] |> List.rev
                
                
   let parseSectionedFileTS filename = 
       
       try
         let lines = filename |> File.ReadAllLines |> List.ofArray ||~> (fun s -> s.Trim())
         
         let is_section_name (x : string) = x.EndsWith ":"
         let is_sections_end = (=) "end."
         
         let rec members = function 
            | x :: xs ->       
               if is_section_name x || is_sections_end x then (x :: xs), [] 
               else let tl, r = members xs in tl, x :: r
               
            | _ -> failwith "unexpected end of file."
         
         let section = function 
            | x :: xs -> let tl,r = members xs in tl, (x, r)
            | _ -> failwith "unexpected end of file."
            
         let rec sections = function 
            | x :: xs when is_section_name x -> 
                                             let tl, (sn, se) = section (x :: xs) 
                                             let tl', r' = sections tl
                                             tl', (r' |> Map.add sn se)
            | x :: xs when is_sections_end x -> xs, Map.empty
            | _ as x -> failwith "section header or a mark of sections end is expected"
         
         let rec all ts = function 
            | (x : string) :: xs -> let tl, s = sections xs                     
                                    seq { yield x,(ts,s); yield! all ts tl }                     
            | [] -> Seq.empty
         
         let ts = File.GetLastWriteTime filename
         
         all ts lines
       with e -> raise (new Exception("while parsing " + filename, e))

   let rec getAllFiles pattern folder  = 
      seq { for x in Directory.GetDirectories folder do yield! getAllFiles pattern x 
            for x in Directory.GetFiles(folder, pattern) do yield x }
            
   let readSections pattern = getAllFiles pattern >> Seq.collect parseSectionedFileTS >> Seq.toList

   type GenDeps = { 
      clsid    : string
      uses     : string list
      nullable : string list
      creates  : string list
      removes  : string list
   }

   type DvDesc = {
      accessors : string list
      mutators  : string list
      genorder  : string list 
   } with member x.is_dynamic = x.mutators <> []

   type DvExt = {
      name : ProgId
      ext  : ProgId  // Это будет имя генератора, который последний изменял или создавал датавью
                     // непонятно, стоит ли что-то здесь делать, если датавью создается только один раз
   } with override x.ToString() = x.ext .//. (x.name + ".dv")

   let compound_dvs = set ["Areator.CompoundSensor"] 
      
   let flip f x y = f y x

   type Hdr = { interfaces : string list }

   let map_find_e err x xs = 
      try 
         Map.find x xs
      with :? KeyNotFoundException -> failwithf "Key %A not found: %s" x err 

   let try_find k def m  = match Map.tryFind k m with Some x -> x | None -> def

   type Context(deps_folder : string) = 

      let sections pattern = readSections pattern deps_folder

      let get_sections'  f pattern= 
         sections pattern |> Map.ofSeq 
         |> Map.map (fun k v -> try (fst v, (f (snd v))) with e -> raise (new Exception("when processing " + k, e)))

      let generators = lazy get_sections' (fun m ->  { uses     = m.["uses:"]
                                                       nullable = try_find "use_nullable:" [] m
                                                       clsid    = match m.["clsid:"] with 
                                                                  | [x] -> x.Trim() 
                                                                  |  _ -> failwith "exaclty 1 GUID expected in clsid section"
                                                       removes  = try_find "removes:" [] m
                                                       creates  = try_find "creates:" [] m }) "*.generator"
                                      
      let dataviews = lazy get_sections' (fun m -> { accessors = m.["accessors:"]
                                                     mutators = try_find "mutators:" [] m
                                                     genorder = try_find "genorder:" [] m }) "*.dv"
                                                
      let headers = lazy get_sections' (fun m -> { interfaces = m.["interfaces:"]}) "*.hdr"
      
      let getLatestTs m = m |> demap |~> (snd >> fst) |> Seq.max
      
      let parseArg (x : string) = match x.Split('=') with 
                                  | [|key; value|] -> key.Trim(), value.Trim() 
                                  | _ -> failwithf "argument string expected in form key=value got %s" x
      
      let arguments = lazy (Map.ofList(readSections "pipeline.args" deps_folder).["Arguments"] 
                             |> snd |> demap 
                             |~> (fun (k, v) -> k.Trim().TrimEnd(':'), v ||~> parseArg |> map)
                             |> map)
                             
      let latestTS = lazy ([  getLatestTs !~generators 
                              getLatestTs !~dataviews 
                              getLatestTs !~headers
                           ] @ ((getAllFiles "pipeline.args" deps_folder |~> File.GetLastWriteTime) |> Seq.toList)
                           |> Seq.max)
                             
     
      let extract_ifaces f = !~dataviews 
                               |> demap 
                               |> Seq.collect (function (dv, (_, desc)) -> desc |> f ||~> (fun iface -> iface,dv)) 
                               |> Seq.groupBy fst
                               |> map
                               |> Map.map (fun _ -> Seq.map snd  >> list)
      
      let iface_to_file = lazy (!~headers
                                 |> demap
                                 |> Seq.collect (fun (file, (_, {interfaces = ifaces})) -> [for i in ifaces do yield i,file])
                                 |> map)
         
      let am = lazy( let accessors = extract_ifaces (function { accessors = A } -> A)
                     let mutators = extract_ifaces (function { mutators = M } -> M)
                     
                     // Если интерфейс указан в одной датавьюхе как аксессор, а в другой - как мутатор, сильно ругаться. 
                     mutators 
                        |> Map.toList
                        ||~> fst
                        ||?> accessors.ContainsKey
                        ||~> (fun k -> sprintf "Interface %s is accessor in %A and mutator in %A" 
                                                 k (list accessors.[k]) (list mutators.[k]))
                                                 
                        |> (function [] -> () | x -> failwithf "%A" x)
                     accessors, mutators)
                     
      let resolve_unique_iface' m iface = 
         match Map.find iface m with [x] -> x 
                                   | dvs -> failwithf "Ambigous resolutions for interface %s found: %A" iface dvs

      let is_unique_iface' m iface = match Map.find iface m with [_] -> true | _ -> false
         
      let check_iface_dv' dv m iface  = 
         m |> Map.find iface 
           |> List.exists ((=) dv) 
           |> (fun x -> if not x then failwithf "interface %s is not supported by %s" iface dv)                             

      let accessors() = fst(!~am)
      let mutators() = snd(!~am)

      let is_accessor x = accessors().ContainsKey x
      let is_mutator x = mutators().ContainsKey x
                     
      let resolve_unique_iface' m iface = 
         match Map.find iface m with [x] -> x 
                                   | dvs -> failwithf "Ambigous resolutions for interface %s found: %A" iface dvs

      let is_unique_iface' m iface = match Map.find iface m with [_] -> true | _ -> false
         
      let check_iface_dv' dv m iface  = 
         m |> Map.find iface 
           |> List.exists ((=) dv) 
           |> (fun x -> if not x then failwithf "interface %s is not supported by %s" iface dv)                             
           
      let dispatch f iface = 
         if is_accessor iface then f (accessors()) iface 
         elif is_mutator iface then f (mutators()) iface 
         else failwithf "Interface %s hasn't been found in dataviews" iface
         
      member x.Generators = !~generators
      member x.Dataviews = !~dataviews
      member x.Headers = !~headers
      member x.InterfacesToFiles = !~iface_to_file
      member x.Accessors = fst(!~am)
      member x.Mutators = snd(!~am)
      
      member x.IsAccessor = x.Accessors.ContainsKey
      member x.IsMutator = x.Mutators.ContainsKey
      
      member x.GeneratorArg progId = try_find progId (Map.ofList []) (!~arguments)
      
      member x.LatestTS = !~latestTS
      
      member x.FindHeaderByInterface iface = 
         match x.InterfacesToFiles.TryFind iface with 
         |  Some hdr -> hdr
         |  None     -> match iface.Split([|"::"|], StringSplitOptions.RemoveEmptyEntries) with 
                        | [|"arena";x;"pl";"IContainer"|] -> "_data/" + x + "Layer.h"
                        | x -> failwithf "cannot find header file for %s" iface 
      
      member x.ResolveUniqueInterface = dispatch resolve_unique_iface' 
      member x.IsInterfaceUnique = dispatch is_unique_iface'
      member x.CheckInterfaceInDataView dv = dispatch (check_iface_dv' dv) 

      member x.QualifyInterface (xs : string seq)  = 
         Seq.map (fun (s : string) -> 
                              match s.Split([|" from "|], StringSplitOptions.RemoveEmptyEntries) with 
                              | [|iface|]     -> (iface, x.ResolveUniqueInterface iface) 
                              | [|iface; dv|] -> (iface.Trim(), dv.Trim())
                              | s -> failwithf "Severals 'from' in a uses section line: %A" s) xs
      
      member x.Generator' = memoize (fun s -> let (ts,g)=x.Generators.[s] in GeneratorCtx(s, ts, g, x))
      
      member x.Generators' = seq { for e in x.Generators do yield x.Generator' e.Key}
      
   and GeneratorCtx(name : string, ts : DateTime, desc : GenDeps, ctx : Context) = 

      let uses_src =     set desc.uses
      let nullable_src = set desc.nullable
      let creates_dvs =  set desc.creates
      let removes_dvs =  set desc.removes
      
      let uses =     lazy (uses_src |> ctx.QualifyInterface)
      let nullable = lazy (nullable_src |> ctx.QualifyInterface)
      
      let uses_dvs =     lazy (!~uses |~> snd |> set)
      let nullable_dvs = lazy (!~nullable |~> snd |> set |> (+) creates_dvs)

      let input_src = uses_src + nullable_src

      let iface_to_dv = lazy (ctx.QualifyInterface input_src |> list) 

      let unique_iface x = !~iface_to_dv |~> fst |?> ((=) x) |> Seq.length |> (=) 1  
      
      let unique_interfaces = lazy (!~iface_to_dv ||?> (fst >> unique_iface))
      
      let headers = lazy (!~iface_to_dv |~> (fst >> ctx.FindHeaderByInterface) |?> ((<>) "") |> set)
      
      let dvs = lazy (!~iface_to_dv |~> snd |> set) 
      
      let is_dv_compound = compound_dvs.Contains
      let is_dv_static dv = snd(ctx.Dataviews.[dv]).is_dynamic = false && not(is_dv_compound dv)
      let is_dv_dynamic dv = not (is_dv_static dv) && not (is_dv_compound dv)
      
      let elaborateStaticDv dv =
         match snd(ctx.Dataviews.[dv]).genorder with
         | [] -> "", ""
         | genorder -> 
            match List.tryFindIndex ((=) name) genorder with
            |  None -> failwithf "Generator %s doesn't appear in 'genorder' section for %s" name dv
            |  Some 0 -> "", List.head genorder
            |  Some i -> List.nth genorder (i-1), List.nth genorder i 
         
      
      //iface_to_dv |> Seq.iter (unp (flip ctx.CheckInterfaceInDataView))
      
      let input_ts = lazy (!~dvs |~> (flip Map.find ctx.Dataviews >> fst)
                                 |>   Seq.fold max ts)

      let used_static_dvs =  lazy (!~uses_dvs - creates_dvs + removes_dvs
                                    |?> is_dv_static
                                    |~> (fun dv -> { name = dv; ext = elaborateStaticDv dv |> fst }) |> set)
                                   
      let created_static_dvs = lazy (creates_dvs 
                                    |?> is_dv_static 
                                    |~> (fun dv -> { name = dv; ext = elaborateStaticDv dv |> snd}) |> set)
                                    
      let created_dynamic_dvs = lazy(creates_dvs |.?> is_dv_dynamic)
      
      let used_dynamic_dvs = lazy(!~uses_dvs - creates_dvs |.?> is_dv_dynamic)
      
      let created_compund_dvs = lazy(creates_dvs |.?> is_dv_compound)
      
      member x.ProgId : ProgId = name
      member x.ClassId : CLSID = desc.clsid
      member x.TimeStamp = ts
      member x.Uses = list uses_src 
      member x.Creates = list creates_dvs 
      member x.Nullable = list nullable_src 
      member x.Removes = removes_dvs 
      member x.InputTimeStamp = !~input_ts
      member x.Ctx = ctx
      member x.IsNullable = (!~nullable_dvs).Contains 
      member x.Headers = list !~headers 
      member x.InputDataViews = list !~dvs 
      member x.InputInterfaces = !~iface_to_dv
      member x.UniqueInputInterfaces = !~unique_interfaces  
      
      member x.UsedStaticDvs = !~used_static_dvs
      member x.CreatedStaticDvs = !~created_static_dvs
      member x.CreatedDynamicDvs = !~created_dynamic_dvs
      member x.UsedDynamicDvs = !~used_dynamic_dvs
      member x.CreatedCompoundDvs = !~created_compund_dvs

