namespace FsSerialization.v3

   open System
   //open NUnit.Framework
   
   type Point_2 = Transas.MegaChart.ChartCanvas.Point_2
   type GeoPoint_2 = Transas.MegaChart.ChartCanvas.GeoPoint_2
   type Vector_2 = Transas.MegaChart.ChartCanvas.Vector_2
   open Transas.Areator.Support.Serialization.Binary.Internal
   open Transas.Areator.UserData.Build
   type Range = Transas.Utils.Range


   type MutableSet<'a> = System.Collections.Generic.HashSet<'a>
   type MutableList<'a> = System.Collections.Generic.List<'a>
   type MutableDict<'a,'b> = System.Collections.Generic.Dictionary<'a,'b>
   type SortedDictionary<'a,'b> = System.Collections.Generic.SortedDictionary<'a,'b>
   type SceneVersion = Transas.Areator.Support.Serialization.Binary.SceneVersion
   
   type ReferenceSet<'a> = Transas.Utils.Collections.ReferenceSet<'a>

   type ReflectionTools = Transas.Utils.ReflectionTools 
   open System.Diagnostics 

   /// "weak" type name
   type TypeName       = string
   /// assembly identifying string
   type AssemblyName   = string 
   /// strong type name
   type StrongName     = TypeName * AssemblyName
   
   /// fully qualifies a type name
   type TypeIdentifier = 
    TypeId of StrongName * Generics
      override x.ToString() = match x with | TypeId(sn, generics) -> fst(sn).ToString() + sprintf "%A" generics
      member x.StrongName = match x with | TypeId(sn, _) -> sn
      member x.Generics = match x with | TypeId(_, generics) -> generics 
   and Generics = TypeIdentifier list
   
   /// identifies expected static type for an object field / a collection element 
   type GenericTypeId = 
      | GTypeId of TypeIdentifier      // a reference type (object/collection). we need to know only TypeId in order to create type instances 
      | TypeValue of ObjTypeDesc       // a value type. we need the list of its members since their appears in the input stream untagged
      | TypeEnum of EnumTypeDesc       // an enum type
      | TypeNullable of GenericTypeId  // nullable type
      | TypeString     
      | TypeGuid       
      | TypeBool       
      | TypeChar       
      | TypeSByte      
      | TypeByte       
      | TypeShort      
      | TypeUShort     
      | TypeInt        
      | TypeUInt       
      | TypeInt64      
      | TypeUInt64   
      | TypeIntPtr  
      | TypeFloat      
      | TypeDouble     
      | TypeDecimal    
      | TypePtArray        // PtArray is a type introduced for performance reasons and is equal to List<Point_2> where Point_2 = double * double
      | TypeDEM            // DEM is a type introduced for performance reasons and is equal float32[,]
      | TypeAny            // special value: no constraints on static type
      override x.ToString() = sprintf "%A" x
            
   /// Type descriptor for enumeration types: type name and list of possible values
   and  EnumTypeDesc = TypeIdentifier * list<string * EnumValue>
   and  EnumValue    = int 
   // NB! Type information in descriptors is to be used only for early type checking
   //     for example, to detect an assignment of an incompatable value to a field

   /// a descriptor for an element of object type.
   /// allows to create an object type instance and populate it by members
   and  ObjTypeDesc  = 
      | ObjType of ObjTypeImpl 
      member x.Value = match x with | ObjType(v) -> v
      
   and  BaseTypeDesc = ObjTypeDesc option 
   and  ObjTypeImpl = TypeIdentifier * BaseTypeDesc * ObjMemberDescs
   and  ObjMemberDescs = list<string * GenericTypeId> /// NB! GenericTypeId can become outdated when converters are applied and this is ok
   
   type ElementType = GenericTypeId
   type KeyType     = GenericTypeId
   // for sequences we store an actual sequence type and type of its elements for typechecking
   type SeqTypeDesc  = TypeIdentifier * ElementType
   type DictTypeDesc = TypeIdentifier * KeyType * ElementType
   type ArrayDesc    = ElementType  
   /// extents of a 2d array
   type QuadDim      = int * int

   /// Id of an object of reference type (object, sequence, dictionary, array, quadarray, dem, ptarray)      
   /// If id >= 0 it means that the reference is defined by the input data and the referenced element is held in 'references' array
   /// id == -1 corresponds to the "instance" of System.Object
   /// id < -1 for freshly created references for elements held in 'fake_references' array
   type RefId = int
   
   
   /// Represents a single node of a serialized object graph
   type Element = 
      |  Reference   of RefId    // index of a referenced object in a 'references' or 'fake_references' array
      |  Enum        of EnumTypeDesc * EnumValue
      |  String      of string
      |  Guid        of System.Guid
      |  Bool        of bool
      |  Char        of char
      |  SByte       of sbyte
      |  Byte        of byte
      |  Short       of int16
      |  UShort      of uint16
      |  Int         of int32
      |  UInt        of uint32
      |  Int64       of int64
      |  UInt64      of uint64
      |  IntPtr      of IntPtr
      |  Float       of System.Single
      |  Double      of double
      |  Decimal     of decimal
      |  Null
      override x.ToString() = sprintf "%A" x
      
   /// an element can be held in 'references' or 'fake_references' arrays
   type RefElement = 
         // nb! object fields are spread into multiple elements each of them corresponds to a base class of the object
         //          type descriptor | reference to 'base' object | field values   
      |  Object      of ObjTypeDesc  * RefId * list<Element>   
      |  Sequence    of SeqTypeDesc  * list<Element>
      |  Dictionary  of DictTypeDesc * list<Element * Element>
      |  Array       of ArrayDesc    * list<Element>
      |  QuadArray   of ArrayDesc    * MutableList<Element> * QuadDim
      |  PtArray     of MutableList<Point_2>
      |  DEM         of float32[,]
      
      
   type IEnumerator<'a> = System.Collections.Generic.IEnumerator<'a>
      
   module memoize_ops = 

      let memoize_on (t : MutableDict<'a,'b>) (f: 'a -> 'b) = 
         fun n ->
            if t.ContainsKey(n) then t.[n]
            else let res = f n
                 t.Add(n,res)
                 res    
    
      let memoize (f: 'a -> 'b) = memoize_on (new MutableDict<'a,'b>()) f

   open memoize_ops   
   
   type Size = int

   /// an input byte stream is split into tokens that are to be fed to a parser to create an object graph
   type Token = 
      |  TkObject     of ObjTypeDesc * RefId       // header for an object. fields are given in following tokens
      |  TkList       of SeqTypeDesc * RefId * Size // header for list
      |  TkSet        of SeqTypeDesc * RefId * Size // header for set
      |  TkDictionary of DictTypeDesc * RefId * Size  // header for dictionary
      |  TkArray      of ArrayDesc * RefId * Size        // header for 1D array     
      |  TkQuadArray  of ArrayDesc * RefId * QuadDim // header for 2D array
      |  TkValueType  of ObjTypeDesc                     // header for value type
      |  TkEnum       of EnumTypeDesc * int              // a value of an enum type
      |  TkNull                                          // null reference
      |  TkReference  of RefId                           // a reference to a reference type instance
      |  TkPtArray    of RefId * MutableList<Point_2> // List<Point_2> 
      |  TkDEM        of RefId * float32[,]               // float[,]
      |  TkBool       of bool
      |  TkChar       of char
      |  TkSByte      of sbyte
      |  TkByte       of byte
      |  TkShort      of int16
      |  TkUShort     of uint16
      |  TkInt        of int32
      |  TkUInt       of uint32
      |  TkInt64      of int64
      |  TkIntPtr     of IntPtr
      |  TkUInt64     of uint64
      |  TkFloat      of System.Single
      |  TkDouble     of double
      |  TkDecimal    of decimal
      |  TkString     of string
      |  TkGuid       of System.Guid
      override x.ToString() = sprintf "%A" x 
      

   module impl =
   
    /// tokenizes input byte stream 
    let tokenize(stream : BinaryStream) =    
      
      let typetable = stream.Types
      let reader = new IO.BinaryReader(stream.Input.OpenRead("data"), Text.Encoding.UTF8)
      
      /// given TypeDescriptor creates a TypeIdendifier and memoizes it in typeids
      let rec getTypeIdentifier = 
      
         memoize(fun (t : TypeDescriptor) -> 
                  TypeId((t.Name, t.AssemblyName), [for i in t.Generics -> getTypeIdentifier i]))
            
      let rec getGenericTypeId (ty : TypeDescriptor) =
         match ty.Name with 
         |  "System.Object" -> TypeAny
         |  "System.Nullable`1" -> TypeNullable(getGenericTypeId(ty.ElementsType))
         | _ -> match ty.KnownType with 
                | KnownType.Object     -> GTypeId(getTypeIdentifier ty)
                | KnownType.Array      -> GTypeId(getTypeIdentifier ty)
                | KnownType.QuadArray  -> GTypeId(getTypeIdentifier ty)
                | KnownType.MultiArray -> failwith "shouldn't be requested"
                | KnownType.Set        -> GTypeId(getTypeIdentifier ty)
                | KnownType.List       -> GTypeId(getTypeIdentifier ty)
                | KnownType.Dictionary -> GTypeId(getTypeIdentifier ty)
                | KnownType.ValueType  -> TypeValue(getObjTypeDesc ty)
                | KnownType.Enum       -> TypeEnum(getEnumTypeDesc ty)
                | KnownType.Guid       -> TypeGuid
                | KnownType.String     -> TypeString
                | KnownType.Bool       -> TypeBool
                | KnownType.Char       -> TypeChar
                | KnownType.SByte      -> TypeSByte
                | KnownType.Byte       -> TypeByte
                | KnownType.Short      -> TypeShort
                | KnownType.UShort     -> TypeUShort
                | KnownType.Int        -> TypeInt
                | KnownType.UInt       -> TypeUInt
                | KnownType.Long       -> TypeInt64
                | KnownType.ULong      -> TypeUInt64
                | KnownType.IntPtr     -> TypeIntPtr
                | KnownType.Float      -> TypeFloat
                | KnownType.Double     -> TypeDouble
                | KnownType.Decimal    -> TypeDecimal
                | KnownType.PtArray    -> TypePtArray
                | KnownType.DEM        -> TypeDEM
                | _ -> failwith "invalid KnownType"
          

      /// given TypeDescriptor of an object type creates ref<ObjTypeDesc> and memoizes it in objtypes            
      and getObjTypeDesc = 

         memoize(fun t -> 
                     let typeid = getTypeIdentifier(t)
                     let basetype = if t.Base.Name = "System.Object" then None else Some(getObjTypeDesc(t.Base))
                     let members = [for f in t.Fields -> (f.Name, getGenericTypeId(f.Type))]
                     ObjType(typeid, basetype, members))

      /// given TypeDescriptor of an enum type creates ref<ObjTypeDesc> and memoizes it in enumtypes
      and getEnumTypeDesc =  

         memoize(fun t -> 
                     let typeid = getTypeIdentifier(t)
                     let values = [for e in t.EnumValues -> (e.Value, e.Key)] 
                     (typeid, values))
               
      let getSeqTypeDesc typeid = getTypeIdentifier(typeid), getGenericTypeId(typeid.ElementsType)
      let getDictTypeDesc typeid = getTypeIdentifier(typeid), getGenericTypeId(typeid.DictionaryKeyType), getGenericTypeId(typeid.ElementsType)
      let getArrayDesc (typeid : TypeDescriptor) = getGenericTypeId(typeid.ElementsType)
      
      let getType()  = TypeDescriptor.Read(typetable, reader)
      let getInt()   = reader.ReadInt32()

      let rec Read = function 
         | TypeString  -> if reader.ReadBoolean() then TkString(reader.ReadString()) else TkNull
         | TypeGuid    -> TkGuid(new Guid(reader.ReadBytes(16)))   
         | TypeBool    -> TkBool(reader.ReadBoolean())   
         | TypeChar    -> TkChar(reader.ReadChar())   
         | TypeSByte   -> TkSByte(reader.ReadSByte())   
         | TypeByte    -> TkByte(reader.ReadByte())   
         | TypeShort   -> TkShort(reader.ReadInt16())   
         | TypeUShort  -> TkUShort(reader.ReadUInt16())   
         | TypeInt     -> TkInt(reader.ReadInt32())   
         | TypeUInt    -> TkUInt(reader.ReadUInt32())   
         | TypeInt64   -> TkInt64(reader.ReadInt64())   
         | TypeUInt64  -> TkUInt64(reader.ReadUInt64())
         | TypeIntPtr  -> TkIntPtr(new IntPtr(reader.ReadInt64()))   
         | TypeFloat   -> TkFloat(reader.ReadSingle())   
         | TypeDouble  -> TkDouble(reader.ReadDouble())   
         | TypeDecimal -> TkDecimal(reader.ReadDecimal())   
         | TypeEnum(ty)-> TkEnum(ty, reader.ReadInt32())
         | TypeValue(t)-> TkValueType(t)
         
         | TypePtArray
         | TypeDEM
         | TypeAny
         | GTypeId(_) 
         | TypeNullable(_)  ->
         
            match reader.ReadByte() |> int32 |> enum<ElementKind>  with 
            
            |  ElementKind.Object ->   let id = getInt()
                                       let obj_desc = getObjTypeDesc(getType())
                                       TkObject(obj_desc, id)
                                       
            |  ElementKind.List  ->    let id = getInt()
                                       let seq_desc = getSeqTypeDesc(getType())
                                       let size = getInt()
                                       TkList(seq_desc, id, size)                                    
            
            |  ElementKind.Set   ->    let id = getInt()
                                       let seq_desc = getSeqTypeDesc(getType())
                                       let size = getInt()
                                       TkSet(seq_desc, id, size)                                    
                                       
            |  ElementKind.Dictionary->let id = getInt()
                                       let dict_desc = getDictTypeDesc(getType())
                                       let size = getInt()
                                       TkDictionary(dict_desc, id, size)                                    
                                       
            |  ElementKind.Array  ->   let id = getInt()
                                       let seq_desc = getArrayDesc(getType())
                                       let size = getInt()
                                       TkArray(seq_desc, id, size)                                    
         
            |  ElementKind.QuadArray-> let id = getInt()
                                       let seq_desc = getArrayDesc(getType())
                                       let width = getInt()
                                       let height = getInt()
                                       TkQuadArray(seq_desc, id, (width, height))   
                                       
            |  ElementKind.MultiArray-> failwith "not supported"
            
            |  ElementKind.Null  ->    TkNull
            
            |  ElementKind.Reference-> TkReference(getInt())
            
            |  ElementKind.ValueType-> let ty = getType()
                                       let obj_desc = getObjTypeDesc(ty)
                                       Read(getGenericTypeId(ty))

            |  ElementKind.DEM ->      let mutable h1 = getInt();
                                       if h1 = 0 then 
                                          TkDEM(-1, null)
                                       else
                                          let mutable id = -1

                                          if h1 = -1 then   // it means that the next word is a reference id
                                             id <- getInt()
                                             h1 <- getInt()

                                          let l1 = getInt()
                                          let h2 = getInt()
                                          let l2 = getInt()

                                          let arr = Array.CreateInstance(typeof<float32>, [|h1 - l1 + 1; h2 - l2 + 1|], [|l1; l2|])

                                          for i1 in l1 .. h1 do
                                             for i2 in l2 .. h2 do
                                                arr.SetValue(reader.ReadSingle(), i1, i2)

                                          TkDEM(id, (arr :?> float32[,]))
                                       
            |  ElementKind.PtArray->   let mutable sz = getInt()
                                       let mutable id = -1;

                                       if sz = -1 then // it means that the next word is a reference id
                                          id <- getInt()
                                          sz <- getInt()

                                       let points = new MutableList<Point_2>(sz)

                                       for i in 0 .. sz-1 do
                                          let y = reader.ReadDouble()
                                          let x = reader.ReadDouble()
                                          points.Add(new Point_2(x, y));

                                       TkPtArray(id, points);
            
            | x -> failwith ("unexpected Element.Kind" + x.ToString())
      
      Read
            
    let parse(tokenizer) = 
   
     let references = new ReferenceSet<RefElement>()
     let fake_references = new ReferenceSet<RefElement>(2)
     
     let putRef(e, id) = 
         if id >= 0 then 
            references.PutObject(e, id)
         else 
            fake_references.PutObject(e, -id)
         Reference(id)
     

     let FakeId() = -fake_references.GetUniqueID()
     let correct(id) = if id = -1 then FakeId() else id 
         
     let rec convert (assignable_to : GenericTypeId) = 
     
         let sequence(seqtypedesc, id, size) =
               let items = [for i in 0..size-1 -> convert (snd seqtypedesc)]
               putRef(Sequence(seqtypedesc,items), id)
               
         let rec obj(objtypedesc, id) = 
               match objtypedesc with 
               |  ObjType(_, basetype, members) -> 
                     let items = [for i in members -> convert(snd i)]
                     let base_obj = match basetype with None -> -1 | Some(b) -> obj(b, FakeId())
                     putRef(Object(objtypedesc, base_obj, items), id) |> ignore
                     id
               
         let token = tokenizer assignable_to
         
         match token with 
         |  TkObject(objtypedesc, id) -> Reference(obj(objtypedesc,id))
         |  TkValueType(objtypedesc) -> Reference(obj(objtypedesc, FakeId()))
         
         |  TkList(seqtypedesc, id, size) -> sequence(seqtypedesc, id, size)
         |  TkSet(seqtypedesc, id, size) -> sequence(seqtypedesc, id, size)
         
         |  TkDictionary(dictTypeDesc, id, size) -> 
               let (_, k_type, e_type) = dictTypeDesc
               let items = [for i in 0..size-1 -> let k = convert k_type in let e = convert e_type in (k,e)]
               putRef(Dictionary(dictTypeDesc,items), id)
               
         |  TkArray(e_type, id, size) -> 
               putRef(Array(e_type, [for i in 0..size-1 -> convert e_type]), id)
         
         |  TkQuadArray(e_type, id, (width, height)) -> 
               let items = new MutableList<Element>(seq {for i in 0..width*height-1 -> convert e_type})
               putRef(QuadArray(e_type, items, (width, height)), id)
         
         |  TkEnum(enumTypeDesc, v) -> Enum(enumTypeDesc, v)
         |  TkNull -> Null
         |  TkReference(id) -> Reference(id)  
         |  TkPtArray(id, v)  -> putRef(PtArray(v), correct(id))
         |  TkDEM(id, v)      -> putRef(DEM(v), correct(id))
         |  TkBool(x)         -> Bool(x)
         |  TkChar(x)         -> Char(x)       
         |  TkSByte(x)        -> SByte(x)
         |  TkByte(x)         -> Byte(x)       
         |  TkShort(x)        -> Short(x)
         |  TkUShort(x)       -> UShort(x)  
         |  TkInt(x)          -> Int(x)
         |  TkUInt(x)         -> UInt(x)
         |  TkInt64(x)        -> Int64(x)
         |  TkUInt64(x)       -> UInt64(x)
         |  TkIntPtr(x)       -> IntPtr(x)
         |  TkFloat(x)        -> Float(x)
         |  TkDouble(x)       -> Double(x)
         |  TkDecimal(x)      -> Decimal(x)
         |  TkString(x)       -> String(x)
         |  TkGuid(x)         -> Guid(x)
   
     let correctFakeReferences() = 
     
         if fake_references.Count > 0 then 
            let mapping = new MutableDict<int,int>()
            for kv in fake_references do 
               let new_id = references.GetUniqueID()
               mapping.Add(-kv.Key, new_id)
               references.PutObject(kv.Value, new_id)
            for k in references.Keys do 
               let map_idx = function | Reference(i) when i < 0 -> Reference(mapping.[i]) | x -> x
               
               let new_e = 
                  match references.GetObject(k) with 
                  |  Object(objtype, baseid, members) ->
                           Object(objtype,(if baseid < -1 then mapping.[baseid] else baseid), members |> List.map map_idx)
                  | Sequence(ty, items) ->
                           Sequence(ty, items |> List.map map_idx)
                  | Dictionary(ty, items) -> 
                           Dictionary(ty, items |> List.map (fun (k,v) -> map_idx(k), map_idx(v)))
                  | Array(ty, items) ->
                           Array(ty, items |> List.map map_idx)
                  | QuadArray(ty, items, ext) ->
                           QuadArray(ty, new MutableList<Element>(items |> Seq.map map_idx), ext)
                  | DEM(x) -> DEM(x)
                  | PtArray(x) -> PtArray(x)
                  
               references.PutObject(new_e, k)
         references
               
     let e = convert TypeAny
     e, correctFakeReferences()
   
   open impl

   module create_object_ops = 
   
      /// returns IEnumarator for IEnumerable
      let getEnumerator<'a>(x : seq<'a>) = x.GetEnumerator()

      /// advances enumerator and returns current element if exists or throw if fails
      let getNext<'a>(it : System.Collections.Generic.IEnumerator<'a>) = 
         if it.MoveNext() then it.Current else failwith "enumeration finished"   
         
      let rec as_pairs = function 
         |  a :: b :: tail -> (a,b) :: as_pairs(tail)
         |  [] -> []
         |  _ -> failwith "odd number of elements"
         
      /// enumerates base classes of ObjTypeDesc 't' (incl. itself) in bottom-up manner                 
      let rec getBasesDownUp t = 
         match t with 
         |  ObjType(_, Some(basetype), _) -> seq { yield t; yield! getBasesDownUp(basetype) }
         |  ObjType(_, None, _) -> seq { yield t }
         
      /// enumerates base classes of 't' : System.Type (incl. itself) in bottom-up manner                 
      let rec getSysBasesDownUp (t : System.Type) = 
         seq { if t <> typeof<obj> then yield t
               if t <> typeof<obj> then yield! getSysBasesDownUp(t.BaseType) }
         
      let getObjTypeMembers  = function | ObjType(_,_,members) -> members 
      
      /// enumerates class member descriptors in its object layout order 
      let getObjectFields = getBasesDownUp  >> Seq.map getObjTypeMembers >> Seq.concat
      
      let formatMissingBinaryFieldExcMsg(typename : string, missing : MutableList<string>) =
         let format = "Binary data for {0} contains field{2}: '{1}' that is missing in metadata."
         let names = String.Join(", ", missing.ToArray());
         String.Format(format, typename, names, if missing.Count = 1 then "" else "s");         
         
      let formatMissingMetaDataFieldExcMsg(typename : string, missing : seq<string>) =
         let names = String.Join(", ", Seq.toArray(missing));
         let format = "Metadata for {0} contains field{2}: '{1}' that were not serialized in binary.";
         String.Format(format, typename, names, if Seq.length(missing) = 1 then "" else "s");
      
      type SerializationException = System.Runtime.Serialization.SerializationException
      type BindingFlags = System.Reflection.BindingFlags
      type MemberInfo = System.Reflection.MemberInfo
      type FormatterServices = System.Runtime.Serialization.FormatterServices
      type IDeserializationCallback = System.Runtime.Serialization.IDeserializationCallback
      
      type MissingFieldInBinaryData(typename : string, missing : MutableList<string>) = 
         inherit SerializationException(formatMissingBinaryFieldExcMsg(typename, missing))
         member x.Name = typename
         member x.Missing = missing
      
      type MissingFieldInMetaData(typename : string, missing : seq<string>) = 
         inherit SerializationException(formatMissingMetaDataFieldExcMsg(typename, missing))
         member x.Name = typename
         member x.Missing = missing |> Seq.toList
         
      /// Given ObjMemberDescs creates an array of MemberInfo[] and checks its correctness against given System.Type
      let getMemberInfoWoBases(members : ObjMemberDescs, systype : System.Type) = 
            let flags = BindingFlags.DeclaredOnly ||| BindingFlags.Instance
                                  ||| BindingFlags.Public ||| BindingFlags.NonPublic
      
            // Expected result:
            // 1. for each m in members find corresponding MemberInfo
            // 2. if it exists return it otherwise null
            
            let sys_members = systype.GetFields(flags) 
                                 |> Seq.filter (fun fld -> fld.IsNotSerialized = false)  
                                 |> Seq.map    (fun fld -> fld.Name, fld)
                                 |> Map.ofSeq
                  
            let in_persistent name = members |> Seq.map fst |> Seq.tryFind ((=) name) |> Option.isSome
            let in_systype = sys_members.ContainsKey
            
            let missing_in_persistent = sys_members |> Map.toList |> List.filter (fst >> in_persistent >> not) |> List.map fst                                                 
            let missing_in_systype = members |> Seq.map fst |> Seq.filter (sys_members.ContainsKey >> not) |> Seq.toList


            let warning x = 
               if Transas.Areator.BuildStatusHolder.Instance <> null then 
                  Transas.Areator.BuildStatusHolder.Instance.Print(Transas.Utils.MessageSeverity.Warning, x)
            
            if MissingFieldsInfo.NeedsToBeProcessed(systype, missing_in_persistent, missing_in_systype) then 
            
               if missing_in_persistent.IsEmpty = false then 
                  warning (sprintf "%A fields exist in persistent data but not in %s" missing_in_persistent systype.Name)
      
               if missing_in_systype.IsEmpty = false then 
                  warning (sprintf "%A fields exist in %s but not in persistent data" missing_in_systype systype.Name)                          
            
            members |> Seq.map (fst >> sys_members.TryFind)
            
      
            
      /// given element 'e' creates .net object. 'references' is a look-up table
      let createObject'(references : ReferenceSet<RefElement>) postCreate = 
      
         /// mapping: RefId -> obj for objects already created
         let createdObjects = new ReferenceSet<obj>()
         
         let getMemberInfoWoBases = memoize getMemberInfoWoBases
         
         let rec createSystemType = 
            memoize(function 
            
                     |  TypeId((name, assembly), generics) -> 
                     
                          let systype = ReflectionTools.GetType(name, assembly)
                          
                          if List.length generics > 0 then
                              systype.MakeGenericType([| for g in generics -> createSystemType g |])
                          else
                              systype
                     )
                     
         let createSystemTypeGeneric = function 
            | GTypeId(typeid) -> createSystemType typeid
            | TypeNullable(x) -> failwith "not implemented so far..."
            | TypeEnum(x)     -> failwith "not implemented so far..."
            | TypeValue(ObjType(ty, _, _))   -> createSystemType ty  
            | TypeString      -> typeof<string>
            | TypeGuid        -> typeof<Guid>
            | TypeBool        -> typeof<bool>
            | TypeChar        -> typeof<char>
            | TypeSByte       -> typeof<sbyte>
            | TypeByte        -> typeof<byte>
            | TypeShort       -> typeof<Int16>
            | TypeUShort      -> typeof<UInt16>
            | TypeInt         -> typeof<Int32>
            | TypeUInt        -> typeof<UInt32>
            | TypeInt64       -> typeof<Int64>
            | TypeUInt64      -> typeof<UInt64>
            | TypeIntPtr      -> typeof<IntPtr>
            | TypeFloat       -> typeof<float32>
            | TypeDouble      -> typeof<double>
            | TypeDecimal     -> typeof<decimal>
            | TypePtArray     -> typeof<MutableList<Point_2>>
            | TypeDEM         -> typeof<float32[,]>
            | TypeAny         -> typeof<obj>
            
         let rec getMembersDownUp_Ex = function 
            |  -1 -> Seq.empty
            |  id -> match references.GetObject(id) with 
                     |  Object(ObjType(_, _, memberdesc), baseid, _) 
                           -> seq { yield memberdesc; yield! getMembersDownUp_Ex baseid }
                           
                     |  _ -> failwith "shouldn't be called"
      
         let rec create = function
         
            |  Reference(id) ->
                  if createdObjects.ContainsID(id) then createdObjects.GetObject(id)
                  else
                     match references.GetObject(id) with 
                     |  Object(ObjType(typeid, basetype, memberdesc), baseid, members) -> 
                           // getting System.Type for the Element
                           let systype = createSystemType(typeid)
                           // creating an uninitialized instance
                           let res = FormatterServices.GetUninitializedObject(systype)
                           
                           // caching it in order to break off circular dependencies
                           createdObjects.PutObject(res, id)
                           
                           // getting MemberInfo[] and checking member consistency 
                           let mi = Seq.zip (getMembersDownUp_Ex id) (getSysBasesDownUp systype)
                                     |> Seq.map getMemberInfoWoBases 
                                     |> Seq.concat 
                                     |> Seq.toArray
                                     
                           /// given id of an object collects its members (from itself and its 'base' objects)
                           let rec collect_members = function
                              | -1 -> []
                              | id ->
                                 match references.GetObject(id) with 
                                 |  Object(_, baseid, members) -> members @ collect_members baseid
                                 |  _ -> failwith "base element is not an object"
                           
                           let collected_members = collect_members id
                           
                           // converting elements to System.Object
                           let members_as_objs = [| for x in collected_members -> create x |]
                           
                           let members_as_objs = 
                              Seq.zip mi members_as_objs 
                                 |> Seq.filter (fst >> Option.isSome)
                                 |> Seq.map snd 
                                 |> Seq.toArray
                                 
                           let mi = mi |> Array.choose (fun x -> x) |> Array.map unbox<MemberInfo>
                           
                           FormatterServices.PopulateObjectMembers(res, mi, members_as_objs) |> ignore
                           
                           match res with    // TODO: call OnDeserialization after complete object graph is deserialized
                              |  :? IDeserializationCallback as ds -> ds.OnDeserialization()
                              |  _ -> ()               
                           
                           let res' = res |> postCreate
                           
                           //let members' = FormatterServices.GetObjectData(res', mi)
                           //FormatterServices.PopulateObjectMembers(res, mi, members') |> ignore
                           
                           res'
                     
                     |  Sequence((typeid, _), items) -> 
                           let systype = createSystemType(typeid)
                           let res = Activator.CreateInstance(systype) 
                           createdObjects.PutObject(res, id)
                           
                           match res with 
                           
                              |  :? System.Collections.IList as L ->
                                       for i in items do 
                                          L.Add(create(i)) |> ignore
                                          
                              |  :? Transas.Utils.Collections.ISet as S ->
                                       for i in items do
                                          S.Add(create(i))
                                       
                              | _ -> failwith "don't know how to create a sequence" // TODO: implement looking for ICollection<T>
                           

                           res 
                     |  Dictionary((typeid, _, _), items) -> 
                     
                           let systype = createSystemType(typeid)
                           let res = Activator.CreateInstance(systype) 
                           createdObjects.PutObject(res, id)
                           
                           match res with 
                              |  :? System.Collections.IDictionary as D ->
                                     for (k,v) in items do
                                       D.Add(create(k),create(v))
                                     
                              |  _ -> failwith "don't know how to create a dictionary"
                           
                           res 
                     
                     |  Array(elem_typeid, items) -> 
                     
                           let elem_type = createSystemTypeGeneric(elem_typeid)
                           let my_type = elem_type.MakeArrayType()
                           let res = Activator.CreateInstance(my_type, [|(Seq.length items :> obj)|] ) :?> System.Array
                           createdObjects.PutObject(res, id)

                           [| for x in items -> create(x)|].CopyTo(res, 0)
                           
                           res :> obj 
                     
                     |  QuadArray(elem_typeid, items, (d1,d2)) -> 
                           
                           //Assert.That(Seq.length items, Is.EqualTo(d1*d2))
                           let elem_type = createSystemTypeGeneric(elem_typeid)
                           let my_type = elem_type.MakeArrayType(2)
                           let res = Activator.CreateInstance(my_type, [|(d1 :> obj);(d2 :> obj)|] ) :?> System.Array
                           createdObjects.PutObject(res, id)

                           for x = 0 to d1-1 do
                              for y = 0 to d2-1 do
                                 res.SetValue(create(items.[x*d2 + y]), x, y)      
                           
                           res :> obj 
                     
                     |  PtArray(x) -> box(x) 
                     |  DEM(x)     -> box(x) 
                     
            |  Enum((typeid, items), value) -> 
                  let systype = createSystemType(typeid)
                  let string_value = items |> Seq.find (fun (s,v) -> v = value) |> fst
                  Enum.Parse(systype, string_value) // TODO: find a simpler way to init an enum
            
            |  String(x)     -> box(x) 
            |  Guid(x)       -> box(x) 
            |  Bool(x)       -> box(x) 
            |  Char(x)       -> box(x) 
            |  SByte(x)      -> box(x) 
            |  Byte(x)       -> box(x) 
            |  Short(x)      -> box(x) 
            |  UShort(x)     -> box(x) 
            |  Int(x)        -> box(x) 
            |  UInt(x)       -> box(x) 
            |  Int64(x)      -> box(x) 
            |  UInt64(x)     -> box(x) 
            |  IntPtr(x)     -> box(x) 
            |  Float(x)      -> box(x) 
            |  Double(x)     -> box(x) 
            |  Decimal(x)    -> box(x) 
            |  Null          -> null
            
         create
      
   open create_object_ops
   

   module converters = 
   
      let transform_objects<'a>(references      : ReferenceSet<RefElement>,
                                type_mask       : StrongName -> bool,
                                type_converter  : ObjTypeImpl -> ObjTypeImpl * 'a,
                                obj_converter   : (ObjTypeImpl * 'a) * list<Element> -> list<Element>) : unit = 
                                
         let converted_types = new MutableDict<ObjTypeImpl, (ObjTypeImpl * 'a)>()
         let converted_objects = new MutableDict<int, RefElement>()
         
         for kv in references do
            match kv.Value with 
            |  Object(ObjType(objtype), id, members) -> 
            
                  let (typeid, _, _) = objtype
                  
                  if type_mask(typeid.StrongName) then
                  
                     if converted_types.ContainsKey(objtype) = false then
                     
                        let converted_type = type_converter(objtype)
                        
                        converted_types.Add(objtype, converted_type)
                     
                     let converted_type = converted_types.[objtype]
                     
                     let converted_obj = obj_converter(converted_type, members)
                     
                     converted_objects.Add(kv.Key, Object(ObjType(fst converted_type), id, converted_obj))
            
            |  _ -> () // we don't process for the moment elements of non-object type                       
         
         for kv in converted_objects do
         
            references.PutObject(kv.Value, kv.Key) 
              
      let replace_by_idx(s : list<'a>, idx : int, newval : 'a) = 
         let rec impl src idx newval i = 
            match src with 
            |  [] -> failwith ("List.length s = " + i.ToString() + "and idx = " + idx.ToString() + " at replace_by_idx")
            |  x :: xs -> if idx = i then newval :: xs else x :: impl xs idx newval (i + 1)
            
         impl s idx newval 0
         
      let replace_by_idx_opt(s : list<'a>, idx : int option, newval : 'a) = 
         match idx with 
         |  Some(i) -> replace_by_idx(s, i, newval)
         |  None -> s
      
         
      let remove_by_idx(s : list<'a>, idx : int) =
     
         let rec impl src idx i = 
            match src with 
            |  [] -> failwith ("List.length s = " + i.ToString() + "and idx = " + idx.ToString() + " at remove_by_idx")
            |  x :: xs -> if idx = i then xs else x :: impl xs idx (i + 1)
         
         impl s idx 0
         
      let find_index (predicate :  'a -> bool) (s : list<'a>) = 
         
         let rec impl src i = 
            match src with 
            |  [] -> failwith "cannot find the element in the list at find_index"
            |  x :: xs -> if predicate(x) then i else impl xs (i + 1)
            
         impl s 0
   
      let try_find_index (predicate :  'a -> bool) (s : list<'a>) = 
         
         let rec impl src i = 
            match src with 
            |  [] -> None
            |  x :: xs -> if predicate(x) then Some(i) else impl xs (i + 1)
            
         impl s 0

      let weakEq a b = fst a = fst b         
         
      let add_field' references filter field_desc member_gen  = 
      
         transform_objects(references, filter, 
            (fun (typeid, basetype, memberdescs) -> ((typeid, basetype, memberdescs @ [field_desc]), ())),
            (fun ((objtype, _),members) -> members @ [member_gen(objtype, members)]))
         
      let change_typename references oldname newname = 
      
         transform_objects(references, weakEq oldname, 
            (fun (typeid, basetype, memberdescs) -> ((TypeId(newname, typeid.Generics), basetype, memberdescs), ())),
            (fun (_,members) -> members))
            
      let replace_type' references typename newtype members_gen = 
         
         transform_objects(references, weakEq typename,
            (fun _ -> newtype),
            (fun (_, members) -> members_gen(members))) 
            
      let remove_field' references filter fieldname = 
      
         transform_objects(references, filter, 
            (fun (typeid, basetype, memberdescs) -> 
               let idx = find_index (fun p -> fst p = fieldname) memberdescs in 
               ((typeid, basetype, remove_by_idx(memberdescs, idx)), idx)),
            (fun ((_, idx), members) -> remove_by_idx(members, idx))) 
            
      let replace_field' references filter oldname newfield_desc field_gen = 
      
         transform_objects(references, filter, 
            (fun (typeid, basetype, memberdescs) -> 
               let idx = find_index (fun p -> fst p = oldname) memberdescs in 
               ((typeid, basetype, replace_by_idx(memberdescs, idx, newfield_desc)), idx)),
            (fun ((objtype, idx), members) -> replace_by_idx(members, idx, field_gen(objtype, members, Seq.nth idx members)))) 
         
      let replace_field_opt' references filter oldname newfield_desc field_gen = 
      
         transform_objects(references, filter, 
            (fun (typeid, basetype, memberdescs) -> 
               let idx = try_find_index (fun p -> fst p = oldname) memberdescs in 
               ((typeid, basetype, replace_by_idx_opt(memberdescs, idx, newfield_desc)), idx)),
            (fun ((objtype, idx), members) -> replace_by_idx_opt(members, idx, field_gen(objtype, members)))) 

      let add_field         references typename = add_field'         references (weakEq typename) 
      let remove_field      references typename = remove_field'      references (weakEq typename) 
      let replace_field     references typename = replace_field'     references (weakEq typename) 
      let replace_field_opt references typename = replace_field_opt' references (weakEq typename) 
      
      let mscorlib = "mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089"
      let nullable x = TypeId(("System.Nullable`1", mscorlib), [x])
      let Transas_Areator_ResourceManager = "Transas.Areator.ResourceManager, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null"
      let Areator_UserData = "Areator.UserData, Version=0.4.0.20585, Culture=neutral, PublicKeyToken=null"//"Areator.UserData, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null"
      let String_id = TypeId(("System.String", mscorlib), [])
      let Float_id = TypeId(("System.Single", mscorlib), [])
      let Int_id = TypeId(("System.Int32", mscorlib), [])
      let Bool_id = TypeId(("System.Boolean", mscorlib), [])
      let Float_nullable_id = nullable Float_id
      let Bool_nullable_id = nullable Bool_id
      let String_Array_id = TypeId(("System.String[]", mscorlib), [])
      let PropertyChangedEventHandler_id = TypeId(("System.ComponentModel.PropertyChangedEventHandler", mscorlib), [])
      
      let Transas_Areator_Utils = "Transas.Areator.Utils, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null"
      let GeoPoint_2_id = TypeId(("Transas.MegaChart.ChartCanvas.GeoPoint_2", Transas_Areator_Utils), [])
      let GeoPoint_2_desc = ObjType(GeoPoint_2_id, None, [("myLatitude", TypeDouble); ("myLongitude", TypeDouble)])
      let Point_2_id = TypeId(("Transas.MegaChart.ChartCanvas.Point_2", Transas_Areator_Utils), [])
      let Point_2_desc = ObjType(Point_2_id, None, [("myY", TypeDouble); ("myX", TypeDouble)])
      
      let DateTime_id = TypeId(("System.DateTime", mscorlib), [])
      let DateTime_desc = ObjType(DateTime_id, None, [("dateData", TypeUInt64)])
      
      let UserSceneAttributes = "Transas.Areator.ContourModel.UserSceneAttributes", Areator_UserData

      let ResourceKey_id = TypeId(("Transas.Areator.ResourceManager.ResourceKey", Transas_Areator_ResourceManager), [])
      let TextureHandle_id = TypeId(("Transas.Areator.UserData.Materials.TextureHandle", Areator_UserData), [])
      let TextureNoise_id = TypeId(("Transas.Areator.UserData.GeneratedLayers.Procobjects.TextureNoise", Areator_UserData), [])

      let ObjectHandle_id x = TypeId(("Transas.Areator.UserData.Resources.ObjectHandle`1", Areator_UserData), [x])
      let SimpleMaterial_id = TypeId(("Transas.Areator.UserData.Materials.SimpleMaterial", Areator_UserData), [])
      
      let List_id x = TypeId(("System.Collections.Generic.List`1", mscorlib),[x])
      let Dict_String_ResourceKey_id = TypeId(("System.Collections.Generic.Dictionary`2", mscorlib),[String_id; ResourceKey_id])
      let ProcObjProtoSettings_id = TypeId(("Transas.Areator.UserData.ProcObjectPrototypes.Settings", Areator_UserData),[])
      let ImageHandle_id = TypeId(("Transas.Areator.UserData.Resources.ImageHandle", Areator_UserData), [])
      let Procobjects_Size_id = TypeId(("Transas.Areator.UserData.GeneratedLayers.Procobjects.Size", Areator_UserData), [])
      
      let ResourceKey_desc = ObjType(ResourceKey_id, None, [("myResourcePath", GTypeId(String_Array_id));("myResourceName", GTypeId(String_id))])
      let TextureHandle_desc = ObjType(TextureHandle_id, None, [("myImage", GTypeId(ResourceKey_id))])
      let TextureHandle_desc_55 = ObjType(TextureHandle_id, None, [("myImage", GTypeId(ResourceKey_id));("myTimeStamp", GTypeId(DateTime_id))])
      let TextureNoise_desc = ObjType(TextureNoise_id, None, [("_texture", GTypeId(ImageHandle_id));("_texture_size", GTypeId(Procobjects_Size_id));("myTimeStamp", GTypeId(DateTime_id)); ("PropertyChanged", GTypeId(PropertyChangedEventHandler_id))])
      let ImageHandle_desc = ObjType(ImageHandle_id, None, [("myResource", GTypeId(ResourceKey_id))])
      let Procobjects_Size_desc = ObjType(Procobjects_Size_id, None, [("_width", TypeFloat); ("_height", TypeFloat); ("myTimeStamp", GTypeId(DateTime_id)); ("PropertyChanged", GTypeId(PropertyChangedEventHandler_id))])
    
      let Dict_String_ResourceKey_desc = (Dict_String_ResourceKey_id, GTypeId(String_id), GTypeId(ResourceKey_id))
      let ProcObjProtoSettings_desc = ObjType(ProcObjProtoSettings_id, None, 
                                        [("<var_presets>k__BackingField",GTypeId(Dict_String_ResourceKey_id));
                                        ("<node_presets>k__BackingField",GTypeId(Dict_String_ResourceKey_id))])
      
      let fromMapping x = TypeId(("Transas.Areator.UserData.Materials.Mapping." + x, Areator_UserData), [])
      
      let SimpleRule_id = fromMapping "SimpleRule"
      let MappingRule_id = fromMapping "MappingRule"

      let TimeStampedList_id x = TypeId(("Transas.Areator.UserData.Materials.Mapping.TimeStampedList`1", Areator_UserData), [x])
      let TimeStampedList_desc x = ObjType(TimeStampedList_id x, None, 
                                       [("myList",      GTypeId(List_id x));
                                        ("myTimeStamp", GTypeId(DateTime_id))])
      
      let genLayerType(name) = ("Transas.Areator.UserData.GeneratedLayers." + name, Areator_UserData)
      
      let nlist(x : seq<'a>) = new MutableList<'a>(x)
      
      let has_name name = function 
         | ObjType(TypeId((x, _), _), _, _) when x = name -> true
         | _ -> false

      let convert'(reader : BinaryStream) postCreate = 
    
         let version = reader.Version
         let (e, references) = reader |> tokenize |> parse 
        
         let putRef e = 
            let id = references.GetUniqueID()
            references.PutObject(e, id)
            Reference(id)
            
         let add_field         = add_field         references
         let remove_field      = remove_field      references
         let replace_field     = replace_field     references
         let replace_field_opt = replace_field_opt references
         let replace_type      = replace_type'     references
         
         let _48() = 
         
            add_field (genLayerType("GeomFenceContourAttributes")) 
                      ("_bump_material", GTypeId(TextureHandle_id))
                      (fun _ -> putRef(Object(TextureHandle_desc, -1, [Null]))) 
                      
                  
            add_field (genLayerType("GeomFenceContourAttributes_nullable"))
                      ("_bump_material", GTypeId(TextureHandle_id))
                      (fun _ -> putRef(Object(TextureHandle_desc, -1, [Null])))
               
         let aPreset() = putRef(Dictionary(Dict_String_ResourceKey_desc, []))
         
         let _49() = 
         
            remove_field UserSceneAttributes "myShadingParams"
            
            add_field UserSceneAttributes 
                      ("myProcObjProtoSettings", GTypeId(ProcObjProtoSettings_id))
                      (fun _ -> putRef(Object(ProcObjProtoSettings_desc, -1, [aPreset(); aPreset()] )))
               
         let _50() = 
            
            replace_field_opt UserSceneAttributes
                              "myProtoSettings" 
                              ("myProcObjProtoSettings", GTypeId(ProcObjProtoSettings_id))
                              (fun _ -> putRef(Object(ProcObjProtoSettings_desc, -1, [aPreset(); aPreset()] )))
               
               
         let _51() = 
         
            add_field   (genLayerType("LakeContourAttributes_nullable"))
                        ("_height", GTypeId(Float_nullable_id))
                        (fun _ -> Null)
                        
            add_field   (genLayerType("LakeContourAttributes_nullable"))
                        ("_adjusted_height", GTypeId(Bool_nullable_id))
                        (fun _ -> Null)

            add_field   (genLayerType("LakeContourAttributes"))
                        ("_height", TypeFloat)
                        (fun _ -> (Float(0.f)))
                        
            add_field   (genLayerType("LakeContourAttributes"))
                        ("_adjusted_height", TypeBool)
                        (fun _ -> (Bool(false)))
         
         let dateTime() = putRef(Object(DateTime_desc, -1, [UInt64(0UL)]))
         
         let _53() = 
         
            add_field   ("Transas.Areator.UserData.ProfileCustomizer.CustomizableProfile`2", Areator_UserData)
                        ("PropertyChanged", GTypeId(PropertyChangedEventHandler_id))
                        (fun _ -> Null)
                        
            add_field   ("Transas.Areator.UserData.UserLayers.Prototypes.LPrototypeRef", Areator_UserData)
                        ("PropertyChanged", GTypeId(PropertyChangedEventHandler_id))
                        (fun _ -> Null)
                        
            let types = 
              Set(
               [
                  "AlphaMaterialContourAttributes";
                  "AlphaMaterialContourAttributes_nullable";
                  "Buildings.Type";
                  "Buildings.Type_nullable";
                  "Buildings.Address";
                  "Buildings.Address_nullable";
                  "Buildings.PanelHouseMaterial";
                  "Buildings.PanelHouseMaterial_nullable";
                  "Buildings.OldHouseMaterial";
                  "Buildings.OldHouseMaterial_nullable";
                  "Buildings.PlaneRoofAttributes";
                  "Buildings.PlaneRoofAttributes_nullable";
                  "Buildings.HippedRoofAttributes";
                  "Buildings.HippedRoofAttributes_nullable";
                  "Buildings.RandomizationAttributes";
                  "Buildings.RandomizationAttributes_nullable";
                  "ChartForestContourAttributes";
                  "ChartForestContourAttributes_nullable";
                  "ChartLineContourAttributes";
                  "ChartLineContourAttributes_nullable";
                  "ChartMaterialRoadsContourAttributes";
                  "ChartMaterialRoadsContourAttributes_nullable";
                  "ChartRailroadsContourAttributes";
                  "ChartRailroadsContourAttributes_nullable";
                  "ChartRiversContourAttributes";
                  "ChartRiversContourAttributes_nullable";
                  "ChartRoadsContourAttributes";
                  "ChartRoadsContourAttributes_nullable";
                  "ChartWaterContourAttributes";
                  "ChartWaterContourAttributes_nullable";
                  "CoastLineContourAttributes";
                  "CoastLineContourAttributes_nullable";
                  "CoastLineElementAttributes";
                  "CoastLineElementAttributes_nullable";               
                  "DetailsContourAttributes";
                  "DetailsContourAttributes_nullable";
                  "EdgeStoneContourAttributes";
                  "EdgeStoneContourAttributes_nullable";
                  "FenceContourAttributes";
                  "FenceContourAttributes_nullable";
                  "FieldContourAttributes";
                  "FieldContourAttributes_nullable";
                  "Filter.Point";
                  "Filter.FlaggedPoint";
                  "Filter.SplineData";
                  "FilterLineContourAttributes";
                  "FilterLineContourAttributes_nullable";
                  "FilterLineElementAttributes";
                  "FilterLineElementAttributes_nullable";
                  "GarageHouseContourAttributes";
                  "GarageHouseContourAttributes_nullable";
                  "GeomFenceContourAttributes";
                  "GeomFenceContourAttributes_nullable";
                  "HeightLineContourAttributes";
                  "HeightLineContourAttributes_nullable";
                  "HeightPointElementAttributes";
                  "HeightPointElementAttributes_nullable";
                  "Hydrants.HydrantOutlet";
                  "Hydrants.HydrantOutlet_nullable";
                  "HydrantElementAttributes";
                  "HydrantElementAttributes_nullable";
                  "InscriptionPointElementAttributes";
                  "InscriptionPointElementAttributes_nullable";
                  "LakeContourAttributes";
                  "LakeContourAttributes_nullable";
                  "MaterialContourAttributes";
                  "MaterialContourAttributes_nullable";
                  "OldHouseContourAttributes";
                  "OldHouseContourAttributes_nullable";
                  "PanelHouseContourAttributes";
                  "PanelHouseContourAttributes_nullable";
                  "ParkingContourAttributes";
                  "ParkingContourAttributes_nullable";
                  "PatternContourAttributes";
                  "PatternContourAttributes_nullable";
                  "PatternInnerAnchorElementAttributes";
                  "PatternInnerAnchorElementAttributes_nullable";
                  "PatternOutterAnchorElementAttributes";
                  "PatternOutterAnchorElementAttributes_nullable";
                  "PipeContourAttributes";
                  "PipeContourAttributes_nullable";
                  "PlanarProcobjectsContourAttributes";
                  "PlanarProcobjectsContourAttributes_nullable";
                  "PlaneAreaContourAttributes";
                  "PlaneAreaContourAttributes_nullable";
                  "PlantingContourAttributes";
                  "PlantingContourAttributes_nullable";
                  "Procobjects.Range";
                  "Procobjects.Range_nullable";
                  "Procobjects.Size";               
                  "Procobjects.Size_nullable";
                  "Procobjects.PlacementParams";
                  "Procobjects.PlacementParams_nullable";
                  "Procobjects.GenerationParams";               
                  "Procobjects.GenerationParams_nullable";               
                  "Procobjects.PointGenerationParams";
                  "Procobjects.PointGenerationParams_nullable";
                  "Procobjects.LightingParams";
                  "Procobjects.LightingParams_nullable";
                  "Procobjects.FriendlyTexture";
                  "Procobjects.FriendlyTexture_nullable";
                  "Procobjects.BillboardParams";
                  "Procobjects.BillboardParams_nullable";
                  "Procobjects.FloatPair";
                  "Procobjects.FloatPair_nullable";
                  "Procobjects.PrototypeParams";
                  "Procobjects.PrototypeParams_nullable";
                  "Procobjects.Variant";
                  "Procobjects.Variant_nullable";
                  "Procobjects.PointVariant";
                  "Procobjects.PointVariant_nullable";
                  "RiverContourAttributes";
                  "RiverContourAttributes_nullable";
                  "RoadContourAttributes";
                  "RoadContourAttributes_nullable";
                  "RoadGraphContourAttributes";
                  "RoadGraphContourAttributes_nullable";
                  "RoadPostElementAttributes";
                  "RoadPostElementAttributes_nullable";
                  "Scene.Material";
                  "Scene.Material_nullable";
                  "Scene.ReliefData.Noise";
                  "Scene.ReliefData.Noise_nullable";
                  "Scene.Relief";
                  "Scene.Relief_nullable";
                  "Scene.Shading";
                  "Scene.Shading_nullable";
                  "Scene.TerrainSettings";
                  "Scene.TerrainSettings_nullable";
                  "Scene.Detailization";
                  "Scene.Detailization_nullable";
                  "Scene.HeightPointsLayer";
                  "Scene.HeightPointsLayer_nullable";
                  "Scene.BuildingsLayer";
                  "Scene.BuildingsLayer_nullable";
                  "Scene.GeospecificMaps";
                  "Scene.GeospecificMaps_nullable";
                  "SideWalkContourAttributes";
                  "SideWalkContourAttributes_nullable";
                  "SpotProcobjectsElementAttributes";
                  "SpotProcobjectsElementAttributes_nullable";
                  "TrenchContourAttributes";
                  "TrenchContourAttributes_nullable";
                  "WaterContourAttributes";
                  "WaterContourAttributes_nullable"
                  "WireContourAttributes";
                  "WireContourAttributes_nullable";
                  "WireElementAttributes";
                  "WireElementAttributes_nullable";
               ] |> List.map (genLayerType >> fst) ) 
            
            add_field'  references
                        (fun x -> types.Contains(fst x))
                        ("myTimeStamp", GTypeId(DateTime_id))
                        (fun _ -> dateTime())
            
                                   
            let types = Set([
                              "Transas.Areator.UserData.Materials.Atlas.AtlasMaterial";
                              "Transas.Areator.UserData.Materials.Atlas.AtlasPartHandle";
                              "Transas.Areator.UserData.Materials.Atlas.AtlasPart";
                              "Transas.Areator.UserData.Materials.Mapping.MappingRule";
                              "Transas.Areator.UserData.Materials.Mapping.Modulator";
                              "Transas.Areator.UserData.Materials.Mapping.SimpleRule";
                              "Transas.Areator.UserData.Materials.Mapping.TerrainMaterialHandle";
                              "Transas.Areator.UserData.Materials.Mapping.TextureEntity";
                              "Transas.Areator.UserData.Materials.Mapping.TextureEntityHandle";
                              "Transas.Areator.UserData.Materials.Mapping.TextureSet";
                              "Transas.Areator.UserData.Materials.Mapping.TextureSet.TextureSetItem";
                              "Transas.Areator.UserData.Materials.SimpleMaterial";
                              "Transas.Areator.UserData.Materials.TextureBump";
                              "Transas.Areator.UserData.Materials.TextureHandle";
                              "Transas.Areator.UserData.Resources.ObjectHandle`1";
                              "Transas.Areator.UserData.ProfileCustomizer.CustomizableProfile`2";
                              "Transas.Areator.UserData.UserLayers.Substrate.DemData";
                              "Transas.Areator.UserData.UserLayers.Prototypes.LPrototypeRef";
                              "Transas.Areator.UserData.UserLayers.Prototypes.PrototypeRef"; 
                              "Transas.Areator.UserData.ProcObjectPrototypes.Settings";              
                            ])   
            
            add_field'  references
                        (fun x -> types.Contains(fst x))
                        ("myTimeStamp", GTypeId(DateTime_id))
                        (fun _ -> dateTime()) 
            
            let replaceField (typename, fieldname, typeid) = 
            
               replace_field  ("Transas.Areator.UserData.Materials.Mapping." + typename, Areator_UserData)
                              fieldname
                              (fieldname, GTypeId(TimeStampedList_id typeid))
                              (fun (_, _, oldvalue) -> 
                                 putRef(Object(TimeStampedList_desc typeid, -1, [oldvalue; dateTime()])))
                            
            [
               "CompositeRule",  "mySubrules",     fromMapping "SimpleRule";
               "TerrainMaterial","myMappingRules", fromMapping "MappingRule";
               "TerrainMaterial","myVertexColors", fromMapping "MappingRule";
               "TerrainMaterial","myModulators",   fromMapping "Modulator";
               "TerrainMaterial","myTextures",     fromMapping "TextureEntity";
               "TerrainMaterial","myTextureSets",  fromMapping "TextureSet";
               "MappingRule",    "myRules",        fromMapping "CompositeRule";
               "TextureSet",     "myTextures",     fromMapping "TextureSet.TextureSetItem";               
            ] |> List.iter replaceField
         
         let fromProcObjects name = "Transas.Areator.UserData.GeneratedLayers.Procobjects." + name, Areator_UserData     
         let withNullable (name, assembly_name) = [(name, assembly_name); (name + "_nullable", assembly_name)] 
                   
         let cartesian xs ys = seq { for x in xs do for y in ys do yield x,y }
         let apply (x,y) = x y
         
         let get_field name = function 
            | Object(ObjType(_, _, memberdesc), _, members) ->
                let idx = memberdesc |> find_index (fun m -> fst m = name) 
                Seq.nth idx members
            | _ -> failwith "a reference to an object expected"
            
         let elem_to_ref = function
            |  Reference(id) ->
                  references.GetObject(id) 
            |  _ -> failwith "a reference expected"
         
         let elem_to_ref_with_id = function
            |  Reference(id) ->
                  references.GetObject(id), id 
            |  _ -> failwith "a reference expected"
            
         let _54() = 
         
            let get name = elem_to_ref >> get_field name
         
            replace_field  ("Transas.Areator.UserData.UserLayers.Prototypes.PrototypeRef", Areator_UserData)
                            "myPrototype"
                           ("myPrototype", GTypeId(ResourceKey_id))
                           (fun (_, _, oldvalue) -> oldvalue |> get "myCustomization" |> get "myPrototype")
            
            cartesian                
               ("GenerationParams" |> fromProcObjects |> withNullable |> List.map remove_field)
               [
                  "_type"; 
                  "_density_noise_period"; 
                  "_density_noise_gamma"; 
                  "_density_noise_clamp_lower"; 
                  "_density_noise_clamp_upper";
                  "_max_lod_dist";
                  "_min_lod_dist";
               ]
               |> Seq.iter apply

            cartesian                
               ("PrototypeParams" |> fromProcObjects |> withNullable |> List.map remove_field)
               [
                  "_special_objects_lighting";
                  "_lighting_params";
                  "_top_normal_angle";
                  "_bottom_normal_angle"
               ]
               |> Seq.iter apply
               
         let _55() = 
         
            let types = Set([
                              "Transas.Areator.UserData.UserLayers.AbstractGeometry.AbstractAttributes";
                              "Transas.Areator.UserData.UserLayers.Bitmaps.BitmapAttributes";
                              "Transas.Areator.UserData.UserLayers.Prototypes.PrototypeAttributes";
                              "Transas.Areator.UserData.UserLayers.Dem.DemElementAttributes";
                            ])   
            
            add_field'  references
                        (fun x -> types.Contains(fst x))
                        ("myTimeStamp", GTypeId(DateTime_id))
                        (fun _ -> dateTime()) 
         
         let refElemOfType name = function 
            | Object(ObjType(TypeId((x, _), _), _, _), _, _) when x = name -> true 
            | _ -> false
            
                        
         let _56() =
         
             let add_tex_field typename fieldname =  
             
                add_field (genLayerType(typename))
                          (fieldname, GTypeId(TextureHandle_id))
                          (fun _ -> putRef(Object(TextureHandle_desc_55, -1, [Null; Null])))
                          
                add_field (genLayerType(typename + "_nullable"))
                          (fieldname, GTypeId(TextureHandle_id))
                          (fun _ -> Null)
         
             cartesian 
               (["Buildings.PanelHouseMaterial"; "Buildings.OldHouseMaterial"] |> List.map add_tex_field)
               ["_floors_night_id"; "_spans_night_id"]
               |> Seq.iter apply

         let dbl = function | Double(x) -> x | _ -> failwith "Double expected"
         
         let toGeoPoint_2 x =
            let lat = x |> get_field "myLatitude" |> dbl
            let lon = x |> get_field "myLongitude" |> dbl
            new GeoPoint_2(lat,lon)
            
         let _57() = 
         
            let apply_to_fst f (a,b) = (f a, b)
            
            let useGaussKruger = false
               
            let convert (sceneCenter, id_to_ignore) = 
               let converter = GeoConverter.Create(useGaussKruger, sceneCenter)
               
               for k in references.Keys do 
                  if k <> id_to_ignore then 
                     let new_e =
                        match references.GetObject(k) with 
                        | Object(ty, baseid, [Double(lat); Double(lon)]) 
                           when ty |> has_name "Transas.MegaChart.ChartCanvas.SimplePoint" -> 
                                    
                              let converted = converter.LatLon2XY(new GeoPoint_2(lat,lon))
                              
                              Object(ty, baseid, [Double(converted.Y); Double(converted.X)])   

                        | Object(ty, baseid, [f1; f2; Double(width); Double(height); f3; f4; f5]) 
                           when ty |> has_name "Transas.Areator.UserData.UserLayers.Dem.DemElementAttributes" ->
                           
                              let pt = new GeoPoint_2(sceneCenter.Latitude + width, sceneCenter.Longitude + height)
                              let converted = converter.LatLon2XY(pt);
                              
                              Object(ty, baseid, [f1;f2;Double(converted.Y);Double(converted.X);f3;f4;f5])                              
                          
                        | PtArray x -> 
                        
                              for i in 0 .. x.Count-1 do
                                 x.[i] <- converter.LatLon2XY(new GeoPoint_2(x.[i].Y, x.[i].X))
                              PtArray x            
                              
                        | _ as x -> x
                     
                     references.PutObject(new_e, k)                   
               
            references.Values 
               |> Seq.tryFind (refElemOfType (fst UserSceneAttributes))               
               |> Option.map (get_field "myCenter" >> elem_to_ref_with_id >> (apply_to_fst toGeoPoint_2) >> convert)
               |> ignore 
               
            add_field UserSceneAttributes 
                      ("myProjection", TypeString)
                      (fun _ -> String(if useGaussKruger then GeoConverter.GaussKruger else GeoConverter.Merkator))
            
            
            ()
            
         let _58() = 
         
            replace_field  UserSceneAttributes
                           "myCenter"
                           ("myCenter", GTypeId(GeoPoint_2_id))
                           (fun (_, _, x) -> 
                                    let pt = x |> elem_to_ref |> toGeoPoint_2 
                                    putRef(Object(GeoPoint_2_desc, -1, [Double(pt.Latitude);Double(pt.Longitude)]))) 
        
         let _59() = 
         
            replace_type   ("Transas.MegaChart.ChartCanvas.SimplePoint", Transas_Areator_Utils)
                           (Point_2_desc.Value, ())
                           (function 
                              | [Double(lat);Double(lon)] -> [Double(lat)(*myY*); Double(lon)(*myX*)]
                              | _ -> failwith "lat;lon expected")
         
         let _60() = 
         
            ["demWidth"; "demHeight"]  
            |> List.iter (fun x -> 
                  add_field ("Transas.Areator.UserData.UserLayers.Dem.DemElementAttributes", Areator_UserData)
                            (x, GTypeId(nullable Int_id))
                            (fun _ -> Null))
         
         let _61() = 
        
            add_field ("Transas.Areator.UserData.GeneratedLayers.PlanarProcobjectsContourAttributes", Areator_UserData)
                      ("_noise_texture", GTypeId(TextureNoise_id))
                      (fun _ -> putRef(Object(TextureNoise_desc, -1, 
                                        [putRef(Object(ImageHandle_desc, -1, [Null])); 
                                         putRef(Object(Procobjects_Size_desc, -1, [Float(0.f); Float(0.f); putRef(Object(DateTime_desc, -1, [UInt64(0UL)])); Null]));
                                         putRef(Object(DateTime_desc, -1, [UInt64(0UL)]));
                                         Null])))
            add_field ("Transas.Areator.UserData.GeneratedLayers.PlanarProcobjectsContourAttributes_nullable", Areator_UserData)
                      ("_noise_texture", GTypeId(TextureNoise_id))
                      (fun _ -> Null)
         
         let info msg = 
            match Transas.Areator.BuildStatusHolder.Instance with null -> () | x -> x.Print(Transas.Utils.MessageSeverity.Info, msg)   
         
         let msg = new System.Text.StringBuilder()
             
         msg.Append("...converting " + version.ToString()) |> ignore
        
         let stages = new SortedDictionary<SceneVersion, (unit -> unit)>()
         
         [         
            (SceneVersion.BumpTextureInGeomFences, _48)
            (SceneVersion.ProcObjProtoSettings, _49)
            (SceneVersion.ProcObjProtoSettingsFix, _50)
            (SceneVersion.LakesHeight, _51)
            (SceneVersion.TimeStampsEverywhere, _53)
            (SceneVersion.LPrototypeRefDestroyed, _54)
            (SceneVersion.MoreTimeStamps, _55)
            (SceneVersion.BuildingsNightTex, _56)
            (SceneVersion.RelativeCoords, _57)
            (SceneVersion.GeoPoint_2, _58)
            (SceneVersion.Point_2, _59)
            (SceneVersion.DemWH_cached, _60)
            (SceneVersion.mcquay_changes, _61)
            
         ] |> List.iter stages.Add
         
         for kv in stages do 
            if kv.Key.CompareTo(version) = 1 then
               msg.Append(" --> " + kv.Key.ToString()) |> ignore
               kv.Value()
               
         info(msg.ToString())
               
         e |> createObject' references postCreate
        
      let convert(reader : BinaryStream) = convert' reader id
      
