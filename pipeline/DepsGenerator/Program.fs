open System
open System.IO
open System.Reflection
open System.Collections.Generic

open Pervasives
open DepsParser
open DepsGraph
open BuildPlanPrinter

let ensure_dir_exists s = 
   let s' = Path.GetDirectoryName(s) 
   if Directory.Exists s' = false then 
      Directory.CreateDirectory s' |> ignore

let input_hash_str = "// input hash = "
let print_hash hashcode    = sprintf "%s%x" input_hash_str hashcode    

let uptodate' filename ts hashcode = 
   if File.Exists filename then 
      if File.GetLastWriteTime filename > ts then true
      else
         use reader = new StreamReader(filename)
         match reader.ReadLine() with 
         | null -> false
         | line when line.StartsWith input_hash_str -> 
               let line = line.Substring(input_hash_str.Length)
               match Int32.TryParse(line, Globalization.NumberStyles.HexNumber, null) with
               | true, hc -> hc = hashcode
               | _ -> false
         | _ -> false               
   else
      false
      
let mutable has_smth_changed = false

let uptodate filename ts hashcode = 
   if uptodate' filename ts hashcode then
      true
   else
      has_smth_changed <- true
      false 


let keys (m : Map<_,_>) = seq { for x in m do yield x.Key }
let remove_keys (m : Map<_,_>) = Seq.fold (flip Map.remove) m 

let remove_equal_keys m1 m2 = 
   let k1 = keys m1 |> Set.ofSeq
   let k2 = keys m2 |> Set.ofSeq
   let bad = Set.intersect k1 k2
   (remove_keys m1 bad), (remove_keys m2 bad) 
   
// TODO: memoize (???)
let norm (name : string) = name.Replace("Areator.","").Replace(".","_") 
   
let unp f = fun (x,y) -> f x y 

let generatorTemplate = "{hash}
#pragma once
#include \"deps_wrappers\\{progid}.h\"

// to implement by user
void process({classname}_ctx &ctx); 

class __declspec ( uuid(\"{clsid}\") ) ATL_NO_VTABLE
   {classname}
      : public CComObjectRootEx<CComSingleThreadModel>
      , public CComCoClass<{classname}, &__uuidof({classname}) >
      , public IPipelineItem
      , public IVarDumperStackImpl
{
public:

   DECLARE_REGISTRY_PROGID(\"{progid}\")

   BEGIN_COM_MAP({classname})
      COM_INTERFACE_ENTRY(IPipelineItem)
      COM_INTERFACE_ENTRY(IVarDumperStack)
   END_COM_MAP()
   
   //{classname} () { build_status::warning() << \"{classname}::{classname}()\";}
   //~{classname} () { build_status::warning() << \"{classname}::~{classname}()\";}

   // IPipelineItem 
public : 
   BOOL __stdcall Process ( IUnknown * dataviews )
   {
      {classname}_ctx ctx = dataviews;
      process(ctx);
      return TRUE;
   }
};
OBJECT_ENTRY_AUTO(__uuidof({classname}), {classname})
         "

try 
   let args = Environment.GetCommandLineArgs()
   
   if args.Length <> 3 then eprintf "Usage: depsgenerator.exe <deps_folder> <cpp_folder>"

   let deps_folder = args.[1]
   let cpp_folder = args.[2]
   
   let ctx = Context deps_folder

   
   let version = 4 
   
   let assembly_ts = Assembly.GetExecutingAssembly().Location |> File.GetLastWriteTime
   
   let cpp_folder_gen = cpp_folder .//. "gen_wrappers"
   Directory.CreateDirectory cpp_folder_gen |> ignore

   let genWrapper (g : GeneratorCtx) =
   
      let output_filename = cpp_folder_gen .//. g.ProgId + ".h"
      
      let ts = max g.TimeStamp assembly_ts
      
      let version = 5 ^^^ version
      
      let hashcode = hash g.ProgId ^^^ hash g.ClassId ^^^ version
      
      let classname = norm(g.ProgId) 
      
      if uptodate output_filename ts hashcode then 
         ()
      else 
         printf "    generating %s..." output_filename
      
         generatorTemplate
          .Replace("{hash}",     print_hash hashcode)
          .Replace("{clsid}",    g.ClassId)
          .Replace("{classname}",classname)
          .Replace("{progid}",   g.ProgId)
         |> (fun s -> File.WriteAllText(output_filename, s))
         printf "done.\n"
   
   let cpp_folder_deps = cpp_folder .//. "deps_wrappers"
   Directory.CreateDirectory cpp_folder_deps |> ignore
   
   let depsWrapper (g : GeneratorCtx) = 

      let output_filename = cpp_folder_deps .//. g.ProgId + ".h"
      
      /// TODO: something more clever with hash i.e. getHash a b c                
      let version = 17 ^^^ version

      /// TODO: Calculate correct hashcode      
      let hashcode = hash g.ProgId ^^^ 
                     hash (map g.InputInterfaces) ^^^ 
                     hash (g.InputInterfaces ||?> (fst >> ctx.IsMutator) |> map) ^^^
                     hash g.Nullable ^^^
                     hash g.Creates ^^^ 
                     hash g.Headers ^^^
                     hash g.Removes ^^^ version 
      
      // TODO: можно было бы вычислять хеш-код шаблона, а не инкрементировать хеш этого кода.        
      
      let ts = max assembly_ts g.InputTimeStamp       
      
      if uptodate output_filename ts hashcode then  
         ()   
      else   
      
         printf "    generating %s..." output_filename

         let cnst iface = if ctx.IsAccessor iface then "const" else ""
         
         let wc f (x,dv) = 
            [if ctx.IsMutator x then 
               yield f "" x dv
             yield f "const" x dv]
         
         let nullify dv = if List.exists ((=) dv) g.InputDataViews then sprintf "%s_ = 0;" (norm dv) else ""
         
         let include_hdr h = sprintf "#include \"%s\"" h 
         let ctor_ini dv = sprintf "       %s(%s_ = getDataView(dvs_,L\"%s\"));" (if g.IsNullable dv = false then "AssertRelease" else "") (norm dv) dv 
         let get_from_dv dv =  sprintf "   IUnknown* get_%s() const { if (%s_ == 0) %s return %s_; }" (norm dv) (norm dv) (ctor_ini dv) (norm dv)
         let get_decl =               "   template <class T> T* get() { int a[-1]; }"
         let get_iface c x dv = sprintf "   template <> %s %s* get<%s %s>(){ return from%s<%s %s>(); }" x c c x (norm dv) c x
         let try_get_iface c x dv = sprintf "   template <> %s %s* try_get<%s %s>(){ return %sPtr(dvs_); }" x c c x x
         let get_dv_decl dv =  sprintf "   template <class T> T* from%s() { int a[-1]; }" (norm dv)
         let get_if_dv c x dv = sprintf "   template <> %s %s* from%s<%s %s>(){ return com_cast<%s>(get_%s()); }" x c (norm dv) c x x (norm dv)
         let member_decl dv  = sprintf "   mutable IUnknownPtr %s_;" (norm dv)
         let add_dv dv     = sprintf "   template <class Stream> void add_%s(Stream & s) { %s Verify(addDataView(dvs_, L\"%s\", s));}" (norm dv) (nullify dv) dv 
         let del_dv dv     = sprintf "   void del_%s() { %s Verify(delDataView(dvs_, L\"%s\")); }" (norm dv) (nullify dv) dv
         let writer dv = [sprintf "struct %s_writer : dataview_writer_ex<%s_writer> " (norm dv) (norm dv)
                          "{";sprintf "   %s_writer (%s_ctx &ctx) : ctx_(ctx) {}" (norm dv) (norm g.ProgId)
                          sprintf "   void do_write(paged_pipeline_ostream & out) { ctx_.add_%s(out); }" (norm dv);
                          "private:"; sprintf "    %s_ctx & ctx_;" (norm g.ProgId); "};"]
            
         List.concat
          [
            [print_hash hashcode]
            ["#pragma once"; "#include \"Pipeline\\standard.h\""; "#include \"Pipeline\\misc.h\""]
            [for x in g.Headers do yield include_hdr x]
            
            ["struct " + norm(g.ProgId) + "_ctx"; "{"]
            ["   pipeline_params const args;"; ""]
            ["   " + norm(g.ProgId) + "_ctx(IUnknown * dvs) : dvs_(dvs), args(dvs)"]
            ["   {}"]
            ["   IUnknown * GetDvs() { return dvs_; }"]
            ["   template <class T> T * get() { int a[-1]; }"]

            [""; "   // Untyped access to dataviews"]; g.InputDataViews ||~> get_from_dv 
            [""; "   // Typed accessor declarations"]; g.InputDataViews ||~> get_dv_decl 
            
            [""; "   // Typed accessors to unique interfaces"]
            g.UniqueInputInterfaces ||~>> (wc get_iface)
            
            [""; "   // Typed accessors to all interfaces"]; 
            g.InputInterfaces ||~>> (wc get_if_dv)
            
            [""; "   // functions to add created dataviews"]; 
            g.Creates ||~> add_dv 
            
            [""; "   // functions to removed unused dataviews"]; 
            g.Removes |.~> del_dv |> list
            
            ["";"private:"; "   IUnknownPtr dvs_;"]
            g.InputDataViews ||~> member_decl 
            ["};"]    
            
            [""; "// dataview_writer_ex to write to output dataviews"]
            g.Creates ||~>> writer  
          ] 
          |> Seq.toArray 
          |> (fun x -> File.WriteAllLines(output_filename,x))     
         printf "done.\n"   
   (*
   let graphGenerator dst src = seq { for x in src do yield x, dst }
      
   let generateGraph(graph : DependencyGraph) = 
      let edges = [for x in graph.DependentOn do yield! graphGenerator x.Key x.Value] 
      let vertices = [for x,y in edges do yield x; yield y] |> Set.of_list
      List.concat 
       [
         ["<?xml version=\"1.0\" encoding=\"utf-8\"?>"]
         ["<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">"]
         [sprintf "  <graph id=\"G\" edgedefault=\"directed\" parse.nodes=\"%d\" parse.edges=\"%d\" parse.order=\"nodesfirst\" parse.nodeids=\"free\" parse.edgeids=\"free\">" (Set.count vertices) (List.length edges)]
         [for v in vertices do yield sprintf "    <node id=\"%s\" />" v]
         [for s,d in edges do yield sprintf "    <edge id=\"0to1\" source=\"%s\" target=\"%s\" />" s d]
         ["  </graph>"]
         ["</graphml>"]
       ] |> Array.of_list |> (fun s -> File.WriteAllLines(@"c:\graph.gml", s))

   let graph = DependencyGraph(ctx, "Areator.VisualExport")
   let stages = list graph.Stages 
   let order = graph.BuildOrder 
   generateGraph(graph)
   
   *)
   
   let depsFolder() = (Assembly.GetCallingAssembly().Location |> Path.GetDirectoryName) .//. "Dp_Deps"

   for kv in ctx.Generators' do 
      try      
         printf "%s\n" kv.ProgId      
         genWrapper kv
         depsWrapper kv
      with e -> raise (new Exception("While processing" + kv.ProgId, e))

   Directory.CreateDirectory(".." .//. "makefiles")  |> ignore
   
   let latestInputTs = ctx.LatestTS
   
   let generators = ctx.Generators |> demap |~> fst |> list

   generators |*> (fun name -> try buildPlanGenerator ctx name (".." .//. "makefiles" .//. name) latestInputTs with e -> printf "... FAILED\n")
      
   
with e -> eprintf "Exception caught: %s" (e.ToString())
          Environment.Exit -1
          