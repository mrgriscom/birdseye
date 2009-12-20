<?

$tmpfile = tempnam("/tmp", "gmapdl-");

$f = fopen($tmpfile, "w");
fwrite($f, "name: " . $_POST['handle'] . "\n");
fwrite($f, "region: " . $_POST['data'] . "\n");
fwrite($f, "layers:\n");
fwrite($f, "  gmap-map:\n");
fwrite($f, "    zoom: " .  $_POST['zoom'] . "\n");
fclose($f);
chmod($tmpfile, 0755);

print "python gmapcache.py < " . $tmpfile;

print "<hr>";

$lines = file($tmpfile);
print "<pre>";
foreach ($lines as $line) {
  print $line;
}
print "</pre>";

?>