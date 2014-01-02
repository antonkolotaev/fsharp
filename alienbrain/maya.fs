namespace Abres

   module Maya = 

      open System
      open System.IO
      open System.Text
      open System.Collections.Generic
      open Printf
      open Microsoft.Win32
      
      open Common
      open NXN
      open Pervasives
      open FileIO
      open CacheableDeps

      let is_maya = ends_with ".mb"

      let maya_registrykey_name = @"SOFTWARE\Autodesk\Archive\Maya\8.5"
      let rk = Registry.LocalMachine.OpenSubKey(maya_registrykey_name)
      
      let appDataPath = 
         let path = Environment.GetEnvironmentVariable("APPDATA") + @"\Transas\Abres\"
         ensure_dir_exists path
         path
      
      let maya_path() = 
         let maya_installdir = rk.GetValue("INSTALLDIR")      
         if maya_installdir = null then failwith "unable to locate mayabatch.exe by registry key " + maya_registrykey_name 
         else unbox<string>(maya_installdir) + @"bin\mayabatch.exe"

      let path_to_underscores (filename : string) = 
         filename.Replace(":","_").Replace("\\","_").Replace("/","_")
         
      let correct_slashes (path : string) = path.Replace("\\", "/")
         
      let temp_deps_path (filename : string) = 
         appDataPath + @"MbDeps\" + path_to_underscores(filename) + ".txt" |> correct_slashes
         
      let change_deps_path (filename : string) = 
         appDataPath + @"MbChangeDeps\" + path_to_underscores(filename) + ".txt" |> correct_slashes
      
      let getMayaReferences' filename = 
         
         eprintf "Maya references for %s: " filename
      
         let tmp_filename = temp_deps_path(filename)
         
         let exists() = 
            let e = File.Exists(tmp_filename)
            eprintf "%s" (if e then "cached" else "not cached")
            e
            
         let up_to_date() = 
            let cache_ts = File.GetLastWriteTime(tmp_filename)
            let file_ts = File.GetLastWriteTime(filename)
            let ok = cache_ts > file_ts
            eprintf " cache_ts = %O %s %O" cache_ts (if ok then ">" else "<") file_ts
            ok
         
         if not (exists() && up_to_date()) then
         
            ensure_dir_exists tmp_filename
            let cmdLine = sprintf "-file \"%s\" -command \"owBuildDependencies(\\\"%s\\\")\"" filename tmp_filename
            
            eprintf "\nrunning %s %s" (maya_path()) cmdLine
            let mayaBatch = createProcess (maya_path()) cmdLine
            
            let buf = new StringBuilder()

            mayaBatch ^> (fun out d -> bprintf buf "%s: %s\n" (if out then "out" else "err") d)
            mayaBatch.WaitForExit()
         
         let refs = tmp_filename |> File.ReadAllLines 
         
         eprintfn "Raw refs are: \n %A" refs
         
         refs
         
      let getMayaReferences path =
      
         //printf "%s\n" (unmap path)

         let mb2ab x =   
            let n = x |?> ((=) ':') |> Seq.length
            match n with 
            |  1 ->  let ab = x |> AB_Path in if ab |> exists then Some(ab) else None
            |  2 -> 
               let tail = x.Substring(x.LastIndexOf(':')+1).Split([|'\\';'/'|])
               let rec loop i acc = 
                  if ABPath(@"\Workspace\" + acc) |> exists then Some(ABPath(@"\Workspace\" + acc))
                  else
                     if i > 0 then loop (i-1) (tail.[i] + @"\" + acc)
                     else None
                     
               loop (tail.Length - 2) tail.[tail.Length-1]   
            | _ -> failwith "1 or 2 colons are expected in a maya reference"
            
         let rec filterExisting' = function 
            | x :: xs -> let p,n = filterExisting' xs
                         match mb2ab x with Some ab -> ab :: p, n | None -> p, UnresolvedMaya x :: n
            | [] -> [], []
         
         let refs, unresolved = 
            path |> unmap |> cacheableDeps 
                                 (getMayaReferences' 
                                    >> Array.toList 
                                    >> filterExisting' 
                                    >> fst 
                                    >> (List.map unmap) ) 
                          |> filterExisting'
         
         ((maya_lib_info path) :: (ab_wrap deps_filename path) :: refs), unresolved
         
      let convertMayaFile mayaTranslations path =       

         let translation = mayaTranslations |~> (fun (s, s') -> s + "\n" + s') |> Seq.toArray

         let dep_filename = change_deps_path (unmap path)
         ensure_dir_exists dep_filename
         
         File.WriteAllLines(dep_filename, translation)  
         
         let cmdLine = sprintf "-file \"%s\" -command \"owChangeDependencies(\\\"%s\\\")\"" (unmap path) dep_filename
         let mayaBatch = createProcess (maya_path()) cmdLine
         let buf = new StringBuilder()

         mayaBatch ^> (fun out d -> bprintf buf "%s: %s\n" (if out then "out" else "err") d)
         mayaBatch.WaitForExit()
         
         printf "%s\n" (buf.ToString())
         
         buf |> ignore
