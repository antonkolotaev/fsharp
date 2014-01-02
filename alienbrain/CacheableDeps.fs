namespace Abres 

   open NXN
   open FileIO
   open Pervasives
   open System.Text
   
   open System.IO
   //open Microsoft.FSharp.Text.Printf
   
   module CacheableDeps = 
   
      let enc = Encoding.GetEncoding 1251
      
      
      let normalize_path = AB_Path >> un_ab
   
      let make_relative_path base' path = make_relative_path' (normalize_path base') (normalize_path path)
         
      let (!.~) f x y = f(x,y)
      let (!..~) f x y z = f(x,y,z)
      
      let writeAllLines filename = 
         ensure_dir_exists filename 
         fun x -> checkout(AB_Path filename); File.WriteAllLines(filename, x, enc)
      
      let writeDeps filename  =   
         eprintfn "writing deps to %s" filename 
         set >> (filename |> Path.GetDirectoryName |> make_relative_path |> Seq.map)
              >>  Seq.toArray >> writeAllLines filename   
      
      let readDeps filename = 
         let base' = (Path.GetDirectoryName filename) + @"\" 
         filename |> (fun x -> File.ReadAllLines(x, enc)) |~> (((+) base') >> Path.GetFullPath)
   
      let deps_filename filename = Path.GetDirectoryName(filename) + @"\deps\" + Path.GetFileName(filename) + ".deps"
      
      let badlinks_filename filename =  Path.GetDirectoryName(filename) + @"\deps\" + Path.GetFileName(filename) + ".badlinks"
      let extlinks_filename filename =  Path.GetDirectoryName(filename) + @"\deps\" + Path.GetFileName(filename) + ".extlinks"
      
      let has_fresh_deps filename = 
         
         let deps_filename = deps_filename filename 
         
         eprintf "Dependencies for %s:" filename
         
         if File.Exists deps_filename then
            let deps_ts = File.GetLastWriteTime(deps_filename)
            let file_ts = File.GetLastWriteTime(filename)
            eprintfn " exists and have ts %O %s %O" deps_ts (if deps_ts > file_ts then ">" else "<") file_ts 
            File.GetLastWriteTimeUtc(deps_filename) > File.GetLastWriteTimeUtc(filename)
         else
            eprintfn " doesn't exist."
            false
      
      let cacheableDeps rawDeps filename = 
      
         let deps_filename = deps_filename filename 
         
         if not(has_fresh_deps filename) then
         
            filename |> rawDeps |> writeDeps deps_filename

         deps_filename |> readDeps |> Seq.toList
      