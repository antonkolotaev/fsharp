namespace Abres

   open NXN.Alienbrain.SDK
   open System
   //open Microsoft.FSharp.Text.Printf

   module NXN =
      
      let nxn = Namespace.GetInstance()
      let mapper = new Mapper()
      
      let pathCmp = StringComparer.InvariantCultureIgnoreCase
      
      
      [<CustomComparison>]
      [<CustomEquality>]
      type ABPathT = 
         | ABPath of string
         member x.get() = match x with ABPath(x) -> x
         override x.Equals(other : obj) = 
            let y = unbox<ABPathT>(other)
            pathCmp.Equals(x.get(), y.get())
         override x.GetHashCode() = x.get().GetHashCode()
         override x.ToString() = "{" + x.get().ToString() + "}"
         member x.concat (y : string) = ABPath(x.get() + y)
         interface IComparable with 
            override x.CompareTo(other : obj) = 
               let y = unbox<ABPathT>(other)
               pathCmp.Compare(x.get(), y.get())
         
      let un_ab (ABPath x) = x
      
      let of_kind kind (x : Item) = x <> null && x.Type = kind
      
      let is_root x   = of_kind @"\" x
      let is_wkspc x  = of_kind @"\Workspace\Workspace\" x
      let is_folder x = of_kind @"\Workspace\DbItem\FileFolder\Folder\Asset\" x
      let is_project x= of_kind @"\Workspace\DbItem\FileFolder\Folder\Project\" x
      let is_folder' x = is_root x || is_wkspc x || is_folder x || is_project x
      let is_file   (x : Item) = x <> null && x.Type.StartsWith @"\Workspace\DbItem\FileFolder\File\Asset\"
      let is_asset x = is_folder' x || is_file x
      
      let get_project_path (ABPath x) = 
         let tokens = x.Split([|'/';'\\'|])
         @"\Workspace\" + tokens.[2] + "\\" |> ABPath

      let maya_lib_info filename = (get_project_path filename).concat(".lib_info")
      let is_maya_lib_info filename = filename = (maya_lib_info filename) 
      let unless_lib_info f filename = if is_maya_lib_info filename = false then f filename
      
      let run (cmd : string) (ABPath ab_path) =
         eprintfn "AB: running %s on %s" cmd ab_path
         if nxn.RunCommand(ab_path, cmd) = false then
            eprintfn "Failed!" 
      
      let checkin    = run "CheckIn"
      let checkout      = unless_lib_info(run "CheckOut")
      let undo_checkout = unless_lib_info(run "UndoCheckOut")
      let get_latest = run "GetLatest"

      let get_latest_not_checked_out (ABPath path) = 
         let cmd = new Command("GetLatest", 0)
         cmd.SetIn("OverwriteCheckedOut", "1")
         eprintfn "AB: running get_latest_not_checked_out on %s" path
         nxn.RunCommand(path, cmd)
      
      let get_property = 
         let t = typeof<Item>.GetMethods().[7]
         (fun (name : string)-> fun (x : Item) -> t.Invoke(x, [|(name :> obj)|]) :?> string)
         
      let to_ignore (x : Item) =  x |> get_property "abres-ignore" = "1"
         
      let set_property = 
         let t = typeof<Item>.GetMethods().[9]
         (fun (name : string)-> fun (value : string)-> fun (x : Item) -> (t.Invoke(x, [|(name :> obj);(value :> obj)|]) |> unbox<bool>))
      
      let has_changed (x : Item) = 
         let server_ts = x |> get_property "SMOT"
         let client_ts = x |> get_property "CMOT"
         client_ts <> "" && server_ts <> client_ts
         
      let locked_by : Item -> string = get_property "Locked By"
      let username  : Item -> string = get_property "UserName"
      let is_checked_out x = locked_by x = username x 
      
      let local_path = get_property "LocalPath"
      let getItem = un_ab >> nxn.GetItem 
      let unmap = getItem >> local_path
      
      let smart_get path = 
         let x = getItem path
         let server_ts = x |> get_property "SMOT"
         let client_ts = x |> get_property "CMOT"
         if client_ts = "" || Int64.Parse(server_ts) > Int64.Parse(client_ts) then
            get_latest path
      
      let AB_Path' = mapper.MapPath >> ABPath
      let AB_Path s =
         let r = AB_Path' s
         r 
         
      let ab_wrap f = unmap >> f >> AB_Path
      
      let exists = function
         |  ABPath(null) -> false
         |  ABPath(ab_path) -> match ab_path |> nxn.GetItem with null -> false | x -> x.IsValid()

      let branchHandle = "595"
      
      let enum_dependencies(ABPath source) = 
      
         let cmd = new Command("NXNWorkspaceExt_Dependency_Get", 0)
         let mutable handle : string = ""
         let mutable b = false
         let mutable deps : ABPathT list = []
         
         while b = false do 
            cmd.SetIn("DependencyHandle", handle)
            cmd.SetIn("DependencyMaxResults", "1")
            cmd.SetIn("DependencyType", "3")
            cmd.SetIn("DependencyConnect", "1")
            cmd.SetIn("DependencySourceBranchHandle", branchHandle)
            
            nxn.RunCommand(source, cmd)
            
            if cmd.WasSuccessful() = true then 
               
               handle <- cmd.GetOutString("DependencyHandle")
               let nres = cmd.GetOutInt("DependencyNumberResults")
               if nres = 1 then
                  let dep = cmd.GetOutString("DependencyTargetNamespacePath_0")
                  let usage = cmd.GetOutString("DependencyUsage_0")
                  if dep <> "" && usage = "Manual" then 
                     deps <- ABPath(dep) :: deps

            b <- handle = ""               
         deps
      
      let add_dependence (ABPath source) (ABPath target) = 

         let cmd = new Command("NXNWorkspaceExt_Dependency_Store", 0)
         cmd.SetIn("DependencySourceBranchHandle", branchHandle)
         
         cmd.SetIn("DependencyTargetNamespacePath", target)
         cmd.SetIn("DependencyTargetBranchHandle", branchHandle)
         
         cmd.SetIn("DependencyType", "3")
         cmd.SetIn("DependencyUsage", "Manual")
         
         nxn.RunCommand(source, cmd)
         
         if cmd.WasSuccessful() = false then
            printf "add_dependence failed on source = '%s' and target = '%s. Reason: %s'\n" 
               source target (cmd.ToString())
            
         //if source |> ABPath |> enum_dependencies |> Seq.exists ((=) (ABPath target)) = false then
         //   printf "add_dependence failed on source = '%s' and target = '%s'\n" source target
         
         cmd |> ignore
         
      let remove_dependence (ABPath source) (ABPath dep) = 
         
         let cmd = new Command("NXNWorkspaceExt_Dependency_Delete", 0)
         cmd.SetIn("DependencySourceBranchHandle", branchHandle)
         
         cmd.SetIn("DependencyTargetNamespacePath", dep)
         cmd.SetIn("DependencyTargetBranchHandle", branchHandle)
         
         cmd.SetIn("DependencyType", "3")
         cmd.SetIn("DependencyUsage", "Manual")
         
         nxn.RunCommand(source, cmd)
         
         if cmd.WasSuccessful() = false then
            failwithf "remove_dependence failed on source = '%s' and target = '%s. Reason: %s'" 
               source dep (cmd.ToString())
               
         cmd |> ignore
