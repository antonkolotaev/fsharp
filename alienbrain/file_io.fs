namespace Abres 

   module FileIO =

      open System
      open System.IO
      open System.Diagnostics
      open System.Collections.Generic

      let pwd = Directory.GetCurrentDirectory
      
      let path_separators = [|Path.AltDirectorySeparatorChar; Path.DirectorySeparatorChar; Path.VolumeSeparatorChar|]
      let split_path (s : string) = s.Split path_separators |> Array.filter ((<>) "")
      
      let createProcess exec args =
        let pi = new ProcessStartInfo(Arguments=args,CreateNoWindow=false,
                     RedirectStandardInput=true, RedirectStandardError=true, RedirectStandardOutput=true,
                     UseShellExecute=false,WorkingDirectory=pwd(),FileName=exec)
        new Process(StartInfo=pi)

      let (^>) (out:Process) (pf: bool -> string -> unit) =
        out.ErrorDataReceived.Add(fun d -> pf false d.Data)
        out.OutputDataReceived.Add(fun d -> pf true d.Data)
        out.Start() |> ignore
        out.BeginErrorReadLine()
        out.BeginOutputReadLine()

      let rec enumFiles dir = 
         seq { for x in Directory.GetDirectories(dir) do yield! enumFiles x;
               for x in Directory.GetFiles(dir) do yield x }
            
      let ensure_dir_exists s = 
         let s' = Path.GetDirectoryName(s) 
         if Directory.Exists s' = false then 
            Directory.CreateDirectory s' |> ignore
            

      let remove_dup_paths xs = new HashSet<_>(xs, StringComparer.InvariantCultureIgnoreCase)

      let make_relative_path' base' path = 
         let base_arr = base' |> split_path
         let path_arr = path  |> split_path
         if base_arr.[0] <> path_arr.[0] then 
            failwith ("calculating relative path supported only for files in a same drive. base = " + base' + ". path = " + path)
            
         let base_depth = base_arr.Length - 1 
         
         let idx = 
            match (Seq.zip base_arr path_arr) |> Seq.tryFindIndex (fun (x,y) -> x.ToLower() <> y.ToLower()) with
            Some idx -> idx | None -> min base_arr.Length path_arr.Length 
         
         let to_up = base_depth - (idx - 1)
         let tail = String.concat @"\" path_arr.[idx..]
         
         (String.replicate to_up @"..\") + tail
