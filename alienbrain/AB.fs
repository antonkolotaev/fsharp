namespace Abres 

   open Printf

   open NXN
   open Common
   open Pervasives
   
   module AB = 
   
      /// Marks a file that its metainfo on dependencies is actual
      let setRefPatchedTs t = 
         t |> set_property "RefPatched" (get_property "CMOT" t) |> ignore
         t |> set_property "RefVersion" current_ts_version |> ignore

      /// Checks whether a file metainfo on dependencies is actual
      let isRefPatched t = 
         eprintf "isRefPatched %A?" t
         let ts_version = get_property "RefVersion" t
         if ts_version <> current_ts_version then 
            eprintfn "ts_version(%s) <> current_ts_version(%s)" ts_version current_ts_version
            false
         else
            let deps_ts = get_property "RefPatched" t
            if deps_ts = null then 
               eprintfn "RefPatched = null"
               false
            else 
               let local_ts = get_property "CMOT" t
               if local_ts <> "" then 
                  eprintf  "comparing with local date..."
                  let ok = deps_ts = local_ts
                  eprintfn (if ok then "same" else "differs") 
                  ok
               else 
                  eprintf "comparing with server date..."
                  let ok = deps_ts = get_property "SMOT" t
                  eprintfn (if ok then "same" else "differs")
                  ok
                        
      let gather_deps ab = 
         ab |> smart_get |> ignore
         ab |> enum_dependencies |*> (remove_dependence ab)
         ab |> add_dependence

      