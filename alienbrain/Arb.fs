namespace Abres 

   open System.IO
   open System.Collections.Generic
   
   open Printf

   open Transas.Areator.Support.Serialization.Binary
   open Transas.Areator.Support.Serialization.Binary.Internal
   open Transas.Areator.UserData
   open Transas.Areator.ResourceManager
   open Transas.Utils
   open Transas.Utils.Settings

   open FsSerialization.v3
   open FsSerialization.v3.impl
   open FsSerialization.v3.converters
   open FsSerialization.v3.create_object_ops
   
   open NXN
   open Pervasives
   open FileIO
   open Common
   open CacheableDeps

   module Arb = 
   
      let is_arb = unmap >> File.OpenRead >> flip using BinaryStream.CheckHeader
      
      let resolutions  = 
         let resourceStorage = new SettingsProperty<ResourceSystem>("Resource Storage", new ResourceSystem());
         let container = SettingsProvider.Instance.DeclareContainer("Areator Properties", resourceStorage);
         let resources = container.GetProperty(resourceStorage)
         fun (rk : obj) -> seq { for x in resources.Storages do yield x.RootContainer.Resolve(rk.ToString()) }

      let (|TypeName|) = function ObjType(TypeId((name, _), _), _, _) -> name

      let convertArbFile objConverter path  = 
         
         let result = 
            use stream = path |> unmap |> File.OpenRead
            if stream |> BinaryStream.CheckHeader then 
               use reader = stream |> BinaryStream.OpenRead
               convert' reader objConverter
            else null
         if result <> null then
            use stream = path |> unmap |> File.OpenWrite
            BinarySerializer.Serialize(result, stream, SceneVersion.Current)
      
      let getArbReferences' path = 
         
         use stream = path |> File.OpenRead
         use reader = stream |> BinaryStream.OpenRead

         let root, references = reader |> tokenize |> parse 
         
         let visitedObjs = new HashSet<int>()
         let gatheredRefs = new HashSet<int * Lazy<string>>()
         
         let lazys = lazy ""
         
         let rec walk (label : Lazy<string>) = function 
         |  Reference id ->
            if visitedObjs.Contains id = false then 
               let fmt_idx i = lazy(sprintf "%s[%d]" (label.Force()) i)
               match id |> references.GetObject with 
               |  Object(ObjType(typeid, basetype, memberdesc), baseid, members) -> 
                     
                     let fmt_field i = lazy (sprintf "(%s : %A).%s" (label.Force()) typeid (Seq.nth i memberdesc |> fst))
                     
                     id |> visitedObjs.Add |> ignore
                     
                     if fst(typeid.StrongName) = "Transas.Areator.ResourceManager.ResourceKey" then
                        (id,label) |> gatheredRefs.Add |> ignore
                        
                     elif fst(typeid.StrongName) <> "Transas.Areator.UserData.UserLayers.AbstractGeometry.AbstractAttributes" then
                        members |> Seq.iteri (fmt_field >> walk)
                        if baseid <> -1 then
                           baseid |> Reference |> walk label
               
               |  Sequence(_, items) ->   items |> Seq.iteri (fmt_idx >> walk)
               |  Dictionary(_, items) -> items |> Seq.iteri (fun i -> (fun (x,y) -> walk (lazy ((fmt_idx i).Force() + ".Key"))  x; 
                                                                                     walk (lazy ((fmt_idx i).Force() + ".Value")) y))                     
               |  Array(_, items) ->      items |> Seq.iteri (fmt_idx >> walk)
               |  QuadArray(_, items, _)->items |> Seq.iteri (fmt_idx >> walk)
               |  PtArray _ -> ()
               |  DEM _     -> ()                     
               
         | _ -> ()
         
         root |> walk (lazy "")
         
         let createObject = createObject' references id
         
         gatheredRefs
            |~> (fun (id,path) -> (id |> Reference |> createObject |> unbox<ResourceKey>), path)

      let resolve' rk res remove_dups = 
         let res = res |> remove_dups |> List.ofSeq
         match res.Length with  
         |  0 -> None
         |  1 -> Some res.[0]
         |  _ -> printf "Multiple resolutions for %A found: %A\n" rk res; Some res.[0]
         
      let resolve_local rk = resolve' rk (rk |> resolutions |?> File.Exists) remove_dup_paths
         
      let resolve rk =  resolve' rk (rk |> resolutions |~> AB_Path |?> exists) set
      
      let removeUnresolvedResources (x : obj) = 
         match x with 
         :? ResourceKey as rk -> 
            match rk |> resolve_local with 
            |  Some s -> x
            |  _      -> null
         |  x -> x
               

      let transformResourceKeys filter (x : obj) = 
         match x with 
         :? ResourceKey as rk -> 
            match rk |> resolve_local with 
            |  Some s -> 
                  match filter s with 
                  |  true, transform -> s |> transform |> ResourceKey.Parse :> obj
                  |  _ -> x
            |  _ -> x
         |  x -> x
         
      let redirectResourceKeys oldsubstr newsubstr  = 
         transformResourceKeys 
            (fun (s : string) -> s.Contains oldsubstr, (fun s -> s.Replace(oldsubstr, newsubstr)))

      let getArbReferences'' = 
      
        /// TODO: Seq.split
        let filter' rs = 
            rs |> List.choose (fst >> resolve),
            rs |> List.choose (fun x -> match resolve (fst x) with None -> Some(UnresolvedKey(fst x, !~(snd x))) | _ -> None)
      
        getArbReferences' >> List.ofSeq >> filter'
         
      let getArbReferences ab_path = 
      
         let local = unmap ab_path
         
         if not(has_fresh_deps local) then
            
            let p,n = getArbReferences'' local
            
            let p = p |> set |> List.ofSeq
            
            p |~> unmap |> writeDeps (deps_filename local)
            
            if not(List.isEmpty n) then 
               n |~> sprintf "%A" |> Seq.toArray |> writeAllLines (badlinks_filename local)
            
            p,n
            
         else 
            let p = local |> deps_filename |> readDeps |~> AB_Path |> List.ofSeq
            p, []
      
            
      (*--  from abres.fsx file --
      let getLocalRefs local_path =  
         use stream =  local_path |> File.OpenRead
         if stream |> BinaryStream.CheckHeader then 
            use reader = stream |> BinaryStream.OpenRead
            reader |> getArbReferences' |> Seq.toList
         else [] *)
      