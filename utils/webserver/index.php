<?php
$directory = getcwd();
echo "<body align= left > \r\n";
echo "\t<font size=5> DL1 Data Check Files from: <strong>$directory</strong> </font><br /><br />\r\n";
$direntry = array();
$path = ".";
$dh = opendir($path);
$i=1;
while (($file = readdir($dh)) !== false) {
    if($file != "." && $file != ".." && strpos($file, ".php") == false && $file != ".htaccess" && $file != "error_log" && $file != "cgi-bin") {
        $direntry[] = $file;
        $i++;
    }
}

sort($direntry);
foreach($direntry as $file)
        if (is_dir($file)) {
               echo "\t<a style=\"color:red\" href='$path/$file'>$file</a><br /><br />\r\n";
        }
        else {
               echo "\t<a style=\"color:blue\" href='$path/$file'>$file</a><br /><br />\r\n";
        }
echo"</body>\r\n";

closedir($dh);
?>
