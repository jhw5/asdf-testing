xquery version "1.0-ml";
import module namespace np = "http://jonesmcclure.com/xq/suggest/nphrases"
  at "/JMP/suggest/source-management/nphrase-management/nphrase-management.xqm";
import module namespace sen = "http://jonesmcclure.com/xq/sentences"
    at "/JMP/search/snippet-management/sentences.xqm";
import module namespace sources = "http://jonesmcclure.com/xq/suggest/sources"
    at "/JMP/suggest/source-management/sources.xqm";
    
declare variable $filename as xs:string external;
declare variable $product as xs:string external;
declare variable $edition as xs:string external;
declare variable $token as xs:string external;
declare variable $batch-id as xs:integer external;

declare variable $PATTERNS1 := ('’|(&apos;)', '(‑)|(&#8209;)', 
                        '(&#8220;)|(&#8221;)','(&#8216;)|(&#8217;)',
                           '(^|\W)EE($|\W)', '(^|\W)EE($|\W)', 
                           '\w{1,3}\.\w{1,3}\.\w{1,3} ', '(\([0-9a-z]\))|[¶§]',
                           "(\$?)\[([A-Za-z,' ]*)\]((')[a-z])?");
declare variable $PATTERNS2 := ("(^|[\W-[\-]])(D)($|[\Ws])", "(^|[\W-[\-]])(P)($|[\Ws])" , 
                           "(^|[\W-[\-]])(W)($|[\W])", "(^|[\W-[\-]])(H)($|[\W])");
declare variable $REPLACEMENTS1 := ("'" , "-", '"', "'", "employee", "employer", 
                                   "", "", "$2");
declare variable $REPLACEMENTS2 := ("$1defendant$3", "$1plaintiff$3", "$1wife$3", "$1husband$3");


try {
  (:let $_sleep := xdmp:sleep(10000000) for testing timeouts:)
  xdmp:log('getting sentences for ' || $filename || ' batch ' || $batch-id),
  let $next-batch := np:document-get-paras-amped($product, $edition, $filename, $batch-id, $token)
  let $paras := if ($batch-id eq 0) then $next-batch[2 to last()] else $next-batch
  let $replace-text-1 as element(replace)* :=
    for $pattern at $index in $PATTERNS1
    return element replace {element pattern {$pattern}, element replacement {$REPLACEMENTS1[$index]}}
  let $replace-text-2 as element(replace)* :=
    for $pattern at $index in $PATTERNS2
    return element replace {element pattern {$pattern}, element replacement {$REPLACEMENTS2[$index]}}
  let $sens :=
    for $para in $paras 
    return sen:parse(element p {sources:replace(sources:replace($para/string(), $replace-text-1), $replace-text-2)})/string()[matches(., '[\w]')] (:do not return punctuation-only sentences:)
  return ( 
    if ($batch-id eq 0) then $next-batch[1] else (),
    $sens,
    if ($batch-id eq 0) then np:has-started($product, $edition, $filename, $next-batch[1], $token) else ())
} catch ($e) {
  np:report-failed($product, $edition, $filename),
  'ERROR', $e//error:code/string()
}