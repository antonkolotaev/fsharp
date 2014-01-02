namespace Abres

   module Common = 
   
      open Transas.Areator.ResourceManager
      open System.Collections.Generic
      open NXN
      
      let current_ts_version = "01-12-09"

      type FilePath = string
            
      type UnresolvedRef = 
         |  UnresolvedKey of ResourceKey * string
         |  UnresolvedMaya of string
         override x.ToString() = 
            match x with 
            |  UnresolvedKey(rk, path) -> rk.ToString() + " at " + path
            |  UnresolvedMaya path -> path
         
      type UnresolvedRefs = List<UnresolvedRef * ABPathT>

      let ends_with' tail (path : string) = path.ToLower().EndsWith(tail)
      
      let ends_with tail = unmap >> (ends_with' tail)
      