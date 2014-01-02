module Pervasives 

   open System.IO
   open System.Collections.Generic

   let (.*.) f g = fun x -> (f x, g x)

   let (|*>)  (xs : seq<'a>) (f : 'a -> unit) = Seq.iter f xs
   let (|?>) (xs : seq<'a>) (f : 'a -> bool) = Seq.filter f xs
   let (|~>) (xs : seq<'a>) (f : 'a -> 'b) = Seq.map f xs

   let (||?>) (xs : list<'a>) (f : 'a -> bool) = List.filter f xs
   let (||~>) (xs : list<'a>) (f : 'a -> 'b) = List.map f xs
   let (||~>>) (xs : list<'a>) (f : 'a -> list<'b>) = List.collect f xs
   
   let (|.?>) (xs : Set<'a>) (f : 'a -> bool) = Set.filter f xs
   let (|.~>) (xs : Set<'a>) (f : 'a -> 'b) = Set.map f xs
   
   let (.||.) f g x = f x || g x
   
   let map = Map.ofSeq
   let demap = Map.toSeq
   
   let flip f x y = f y x
   
   let ifThenElse filter ifBranch elseBranch = 
      (Seq.groupBy filter) >> Seq.iter (function (false, xs) -> xs |> ifBranch | (true, ys) -> ys |> elseBranch)
   
   let make_2_2 f x = x, (f x)
   let make_1_2 f x = (f x), x
   let constant x = fun _ -> x
   let pair_map_2 f (x, y) = (x, f y)
   let map_2nd = pair_map_2
   
   let (+++) x y = Seq.concat [x; y]

   let (.//.) x y = Path.Combine(x,y)

   let memoize_on (cache : Dictionary<_, _>) f = 
       fun x -> 
           let ok,res = cache.TryGetValue(x)
           if ok then res 
           else let res = f x
                cache.[x] <- res
                res
   
      
   let memoize f = memoize_on (new Dictionary<_, _>()) f

   let (!~) (x : Lazy<_>) = x.Force()             
   
   let pairify f x y = f(x,y)
   
   