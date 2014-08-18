dircmp
======

Directory comparison utility

Compares directories 'a' against directories 'b'.  All files in 'a' will
be categorized as a unique or duplicate file based on its presense in 'b'.
This program implements a rdfind-like algorithm to reduce the amount of time
spent md5-scanning whole files.  Parsible output files 'dircmp-results'
will be generated.

--an [DirectoryOfInterest] (Non-recursive search)
--ar [DirectoryOfInterest] (Recursive search)
--bn [CompareDir] (Non-recursive search)
--br [CompareDir] (Recursive search)
     At least one of --ar or --an, and one of --br or --bn is required.
     Multiple --ar,--an,--br,--bn options accepted.

--noprogressbar
     Don't output progress bar
--quiet|-q
     No console output; implies --noprogressbar

--finddupsearly|--ssd
     Sort md5 file-scaning to find duplicates as early as possible.
     Default behaviour is to sort by inode to reduce harddisk thrashing.
--skipmd5
     Skip the full-file md5 scan phase.  Not recommended.
--probables [name|time|nameandtime|none]
     If files match size and mid-md5 check, this will guess to categorize 
     all remaining files before the md5 scan on name or time.  Useful 
     if you skip the md5 scan or break out of it early.
--midsize|--ms [bytes]
     Use custom size for mid-bytes scan.  Default 4096 bytes.

--noresults
     Don't write results to disk.
--resultsdir [directory]
     Change output directory.  Default: current directory.
--delim [delim]
     Use custom delimiter when outputting duplicate files.  Default \t.
--consoleshow|--c [types]
     Types to show in console when found.
     Available: unique, duplicate, duplicate-hardlink,
                probable-unique, probable-duplicate, zero
