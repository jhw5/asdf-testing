xquery version "1.0-ml";

declare default function namespace "http://www.w3.org/2005/xpath-functions";
import module namespace np = "http://jonesmcclure.com/xq/suggest/nphrases"
    at "/JMP/suggest/source-management/nphrase-management/nphrases.xqm",  
    "/JMP/suggest/source-management/nphrase-management/nphrase-management.xqm"; 
    
    
declare variable $docstr as xs:string external;
declare variable $product as xs:string external; 
declare variable $edition as xs:string external;
declare variable $filename as xs:string external;
declare variable $batch-id as xs:string external;
declare variable $token as xs:string external;

try {
  let $nphrases :=
        element phrases {np:build-noun-phrases(xdmp:unquote(normalize-space($docstr), (), 'repair-full'))}
  let $vphrases :=
        element phrases {np:build-verb-phrases(xdmp:unquote(normalize-space($docstr), (), 'repair-full'))}
  return (
    count($nphrases/nphrase1), count($nphrases/nphrase2), count($vphrases/vphrase1),
    np:insert-phrases-batched-amped(element phrases {$nphrases/nphrase1}, 'nphrase1', $product, $edition, $filename, $batch-id, $token),
    np:insert-phrases-batched-amped(element phrases {$nphrases/nphrase2}, 'nphrase2', $product, $edition, $filename, $batch-id, $token),
    np:insert-phrases-batched-amped($vphrases, 'vphrase1', $product, $edition, $filename, $batch-id, $token),
    if (np:batches-complete($product, $edition, $filename) + 1 eq np:batches-total($product, $edition, $filename)) then
        np:has-completed($product, $edition, $filename, $token) else ())
        
} catch ($e) {
  xdmp:log("exception: " || $e//error:code),
  np:report-failed($product, $edition, $filename),
  'ERROR', $e//error:code/string()
  (:fn:error((), $e//error:code):)
}