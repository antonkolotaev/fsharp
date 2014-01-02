namespace Abres

   module Driver =

      open System
      open System.IO
      open System.Diagnostics
      open System.Reflection
      open FsSerialization.v3
      open FsSerialization.v3.impl
      open FsSerialization.v3.converters
      open FsSerialization.v3.create_object_ops
      open Transas.Areator.Support.Serialization.Binary
      open Transas.Areator.Support.Serialization.Binary.Internal
      open Transas.Areator.UserData
      open Transas.Areator.ResourceManager
      open Transas.Utils
      open Transas.Utils.Settings
      open System.Collections.Generic
      open System.Windows.Forms
      open System.Text
      open Printf
      open Microsoft.Win32
      

      open NXN.Alienbrain.SDK
      open Abres.NXN

      open Pervasives
      open Abres.Maya
      open Abres.FileIO
      open Abres.Common 
      open Abres.Arb
      open Abres.AB
      open Abres.CacheableDeps
      
      type UnresolvedRefs = Abres.Common.UnresolvedRefs  
      
      [<AbstractClass>]
      type IFile() = 
          abstract getReferences: List<UnresolvedRef * ABPathT> -> ABPathT list

      let is_tex = ends_with ".bmp" .||. ends_with ".dds" .||. ends_with ".psd"

      let convertFile objConverter mayaConverter path = 
         if is_arb path then convertArbFile objConverter path
         elif is_maya path then convertMayaFile mayaConverter path
         
         
      let getLocalReferences' path = 
         if is_arb path then getArbReferences path 
         elif is_maya path then getMayaReferences path
         else [], []
               
               
      let unresolvedDlg (ABPath path) unres = 
         let msg = unres |~> (sprintf "    %A\n") |> Seq.fold (+) ""
         if msg = "" then true
         else MessageBox.Show("Several resources are not resolved:\n" + msg + "\n in " + path + "Continue check-in?", 
                              "Areator", MessageBoxButtons.YesNo, MessageBoxIcon.Exclamation) = DialogResult.Yes      
         
      let patchRefs ab  = 
         let item = ab |> getItem 
         eprintfn "patchRefs for %s" (unmap ab)
         if not(is_folder item) then
            let gather_deps' = gather_deps ab
            let refs, localUnres  = ab |> getLocalReferences'
            eprintf "patchRefs: local references are: \n %A" refs 
            if unresolvedDlg ab localUnres then
               refs |> set |*> gather_deps'
               item |> setRefPatchedTs   
               true
            else 
               false
         else 
            true

      // gathers references for a given file
      // if a local copy of the file is newer than the AB version, references are computed based on the local copy
      // otherwise references from AB are taken
      let getFreshABReferences ab_path = 
         if ab_path |> patchRefs = false then 
            failwith "failed to update file %s dependencies." (un_ab ab_path)
         ab_path |> enum_dependencies, []
      
      // TODO: walk_local
         
      let rec walk_nxn_folder ab_path = 
         seq {
               let item = ab_path |> getItem
               if item |> to_ignore |> not then  
                  if  item |> is_folder' then
                     for x in item.Children do 
                        let x = x :?> Item
                        if is_asset x then 
                           yield! walk_nxn_folder (ABPath x.Path) 
                  else
                     yield ab_path 
             }
      
      let rec walk_on_references getReferences (visited : HashSet<ABPathT>) ab_path = 
            seq {
                  if not(visited.Contains ab_path) then
                  
                     yield ab_path; visited.Add ab_path |> ignore
                     
                     for x in getReferences ab_path do 
                        yield! walk_on_references getReferences visited x
                }
                  
            
            
         
      let walkRecursive' getReferences f ab_path  = 

         let rec impl (visited : Set<ABPathT>) ab_path : Set<ABPathT> * list<UnresolvedRef*ABPathT> = 
         
            let rec loop visited = function
               | x :: xs ->   eprintfn "looping... %s" (unmap x)
                              let visited,u = impl visited x
                              let visited,u' = loop visited xs 
                              visited, u @ u'
               | [] -> visited, []
            
            if visited.Contains ab_path = false then
               try 
                  let item = ab_path |> getItem
                  eprintfn "processing %s..." (ab_path.get())
                  if item |> to_ignore |> not then  
                     if  item |> is_folder' then
                        let children = [for x in item.Children -> x :?> Item] 
                                          |> List.filter is_asset 
                                          |> List.map ((fun (x : Item) -> x.Path) >> ABPath)
                                          
                        loop (visited.Add ab_path) children
                     else
                        f ab_path 
                        let refs, unresolved = getReferences ab_path
                        let visited, unresolved' = loop (visited.Add ab_path) refs
                        let unresolved = (unresolved |> List.map (fun x -> x, ab_path))
                        visited, (unresolved' @ unresolved)
                  else
                     visited, []
                                             
               with 
                  e -> eprintfn "Exception catched when processing %s: %A\n" (un_ab ab_path) e.Message
                       visited, []
            else
               visited, []
                        
         ab_path |> impl (set [])

      let walkRecursiveNoDeps = walkRecursive' (fun _ -> [],[]) 
         
      let walkRecursiveLocal  = walkRecursive'  getLocalReferences'

      let walkRecursiveFreshAB' = walkRecursive' getFreshABReferences
      let walkRecursiveFreshAB  = walkRecursive' getFreshABReferences
      let walkRecursiveAB       = walkRecursive' (fun x -> enum_dependencies x, [])

      let enum_deps_not_maya path = if not(is_maya path) then getFreshABReferences path else [], []
      let walkRecursiveAB_notMB = walkRecursive' enum_deps_not_maya 


      let check_deps_coherency ab_path = 

         ab_path |> get_latest |> ignore 
         
         let in_ab   = ab_path |> enum_dependencies |> set
         let on_disk = ab_path |> getLocalReferences' |> fst |> set
         
         if on_disk <> in_ab then
            printf "Incoherent dependencies for file %O:\n" ab_path
            let diff a b label = 
               let ds = a - b
               if Set.count ds > 0 then
                  printf "Only %s\n" label
                  ds |> Set.iter (printf "     %O\n")  
                  
            diff on_disk in_ab "on disk"
            diff in_ab on_disk "in ab" 

      let unresolved_to_str unresolved = 
         
         let buf = new StringBuilder()

         for rk, files in unresolved |> Seq.groupBy fst do
            rk    |> bprintf buf "%A:\n" 
            files |*> (fun (missing, ABPath path) ->  bprintf buf "   %s: %O\n" path missing)
            
         buf.ToString()

      let mydocuments_folder = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments)
      let mel_scripts_dir  = Path.Combine(mydocuments_folder, @"maya/scripts")
      
      let source_dir = Assembly.GetCallingAssembly().Location |> Path.GetDirectoryName
      
      let mel_script_paths name = 
         Path.Combine(source_dir, name), 
         Path.Combine(mel_scripts_dir, name)
         
      let copyMelFile name = 
         let src,dst = mel_script_paths name
         eprintf "copy mel script %s --> %s..." src dst
         if dst |> File.Exists |> not 
            || File.GetLastWriteTime(src) > File.GetLastWriteTime(dst) then
               FileIO.ensure_dir_exists dst
               File.Copy(src, dst, true)
               eprintfn " updated" 
         else
            eprintfn " up to date"

      let main'(cmdArgs : string[]) = 
         
         //"attach" |> MessageBox.Show |> ignore
         
         eprintfn "---==== Abres called with arguments \n %A" cmdArgs
         
         let firstArgPos = 1 + (cmdArgs |> Seq.findIndex (fun s -> s.ToLower().EndsWith(".exe")))
         
         ["owBuildDependencies.mel"; "owChangeDependencies.mel"] |*> copyMelFile
            
         let filenameExpected (args : string[]) = 
            if args.Length < firstArgPos + 2 then 
               failwith "filename expected"
               
         let secondArg() = 
            cmdArgs |> filenameExpected
            cmdArgs.[firstArgPos + 1]
            
         if cmdArgs.Length < firstArgPos + 1 then 
            printf "Usage: ab_rec [get|checkin|checkout|undo] filename"
         else
               
            let collectedDeps()      = 
               let res = secondArg() |> AB_Path |> walkRecursiveAB ignore |> fst
               eprintfn "collectedDeps are:"
               res |~> un_ab |*> (eprintfn "%s")
               res
               
            let collectedFreshDeps() = 
               let res = secondArg() |> AB_Path |> walkRecursiveFreshAB ignore |> fst
               eprintfn "collectedFreshDeps are:"
               res |~> un_ab |*> (eprintfn "%s")
               res

            let filesToCommit() = 

               let source = secondArg() |> AB_Path
               let visited, unresolved = source |> walkRecursiveLocal ignore
               
               if unresolvedDlg source unresolved then 
                  visited 
                     |?> (getItem >> is_checked_out)
                     |~> un_ab 
                     |>  set
               else Set.empty
            
            match cmdArgs.[firstArgPos] with 
            
            |  "fresh_deps"| "fd" -> collectedFreshDeps() |~> un_ab |*>  printf "%s\n"
            |  "deps"      | "d"  -> collectedDeps()  |~> un_ab |*>  printf "%s\n"
            
            
            |  "checkout"  | "o" -> collectedFreshDeps() |*>  checkout
            |  "get"       | "g" -> collectedFreshDeps() |*>  get_latest
            |  "undo"      | "u" -> collectedDeps()      |*>  undo_checkout
                        
            |  "checkin"   | "i" -> filesToCommit()
                                       |> FileToCheckInDialog.Show 
                                       |*> (printf "%s\n")
                                       
            |  "commit"    | "c" -> filesToCommit() |~> ABPath |?> patchRefs |*> checkin 
            
            |  "check_coherency" | "cc" -> 
                  
                  let visited, _ = secondArg() |> AB_Path |> walkRecursiveLocal check_deps_coherency
                  ()
                              
            |  "patchref" | "pr" ->
            
                  let ab = secondArg() |> AB_Path
                  
                  let s = if patchRefs ab then "0" else "1"
                  
                  s |> printf "%s"
                  
            |  "integrity" | "ri" ->
            
                  let ab = secondArg() |> AB_Path
                     
                  let visited, unresolved = ab |> walkRecursiveFreshAB' ignore 

                  unresolved |> unresolved_to_str |> printf "%s"
                  
                  ()
                  
            |  "find_unreferenced" | "fu" -> 
            
                  let where_to_seek = 
                     if cmdArgs.Length < firstArgPos + 3 then nxn.Root.Path |> ABPath else cmdArgs.[firstArgPos + 2] |> AB_Path

                  let reachable_from_root = secondArg() |> AB_Path |> walkRecursiveAB ignore |> fst |> set
                  
                  let visited = where_to_seek |> walk_nxn_folder 

                  let set_color = set_property "Color" 
                  
                  for x in visited do 
                     let item = getItem x
                     item |> set_color (if reachable_from_root.Contains x then 
                                          if isRefPatched item then "darkBlue" else "darkRed"
                                        else "darkGreen") |> ignore
                  (*                      
                  for x in visited do 
                     x |> getItem |> set_property "Color" (if reachable.Contains x then 
                                                               if isRefPatched (getItem x) then "darkBlue" else "darkRed"
                                                           else "darkGreen") |> ignore *)
                                        
            | "tw" ->
            
                  let root = ABPath @"\Workspace\TW content" 
            
                  let last_consistency_ts_file = root |> un_ab |> (.//.) "consistency.timestamp"
                  
                  let last_consistency_ts = last_consistency_ts_file |> File.GetLastWriteTime
                  
                  let is_not_checked_out x = 
                     try 
                        if is_checked_out (getItem x) then 
                           printf "%A is checked out -> eliminating from the check set\n" x
                           false
                        else
                           true
                     with e -> printf "exception caught at is_checked_out %s\n" (un_ab x); false
                     
                  root |> get_latest_not_checked_out
                      
                  let all_files = root |> walk_nxn_folder 
                  
                  let get_files = all_files |?> is_not_checked_out |> Seq.toList
                  
                  let new_files, _ = get_files |> List.partition (unmap >> File.GetLastWriteTime >> (<) last_consistency_ts)

                  printf "Files have changed: "; new_files |*> (un_ab >> (printf "   %s\n"))
                  
                  let is_tw_content (ABPath x) = x.StartsWith(@"\Workspace\TW content")
                  
                  let getArbReferences path = 
                  
                     let local = unmap path
                  
                     let p,n = getArbReferences'' local
                     
                     let p = p |> set |> List.ofSeq
                     let n = n |> set |> List.ofSeq
                                                                                                                  
                     if not(List.isEmpty n) then 
                        printf "%s has some dangling references. They are written to %s" (un_ab path) (badlinks_filename local)
                        n |~> sprintf "%A" |> Seq.toArray |> writeAllLines (badlinks_filename local)
                        
                     let extlinks = p |?> (is_tw_content >> not) |> Seq.toList
                     
                     if not(List.isEmpty extlinks) then 
                        printf "%s has some references out of TW content. They are written to %s" (un_ab path) (extlinks_filename local)
                        extlinks |~> (un_ab >> sprintf "%s") |> Seq.toArray |> writeAllLines (extlinks_filename local)
                        
                     p
                                              
                  
                  ()
                                        
            |  x -> 
            
                  failwith ("unknown command: "+ x)

                  let reachable_from_profiles = secondArg() |> AB_Path |> walkRecursiveAB_notMB ignore |> fst |> set            
                  
                  let textures = reachable_from_profiles |?> is_tex |~> unmap |> set

                  let tex_coding (src : string) = src.Replace(@"Z:\", @"Z:\TW Content\textures\")
                  
                  let copy (src : string) = 
                     let dst = src |> tex_coding
                     Directory.CreateDirectory(Path.GetDirectoryName(dst)) |> ignore
                     File.Copy(src, dst)
                  
                  textures |*> copy
                  
                  let normalize = AB_Path >> unmap
                  let to_rk (src : string) = src.Replace(@"Z:\", @"TW Content\textures\")
                  
                  let filter path = 
                     if textures.Contains(normalize path) 
                        then (true, normalize >> to_rk) 
                        else (false, normalize >> to_rk)
                  
                  secondArg() |> AB_Path |> walkRecursiveNoDeps (convertFile (transformResourceKeys filter) []) |> ignore
                  
                  ()
            
               (*
               for i in (firstArgPos+1)..(cmdArgs.Length-1) do
                  let ab = cmdArgs.[i] |> AB_Path
                  let visited, unresolved = ab |> walkRecursiveFreshAB' (adapt ignore) 
                  unresolved |> unresolved_to_str |> printf "%s" *)
                  
               

               //printf "unknown command %s" cmdArgs.[1]

      let main() = main'(Environment.GetCommandLineArgs())

      //[<STAThread>]
      //do main()

