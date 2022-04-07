A simple(-ish) script for Shane that gathers data about matches in
a 'Learn DotA 2 League' (LD2L) season for him to analyze.

The interesting challenges were in managing rate limiting and caching
of results so that we don't bother the servers nor take too long to
get all of our data.

The script creates a json file in the cwd for caching purposes, in
addition to the output csv.

Important to note: some parts depend on scraping ld2l.gg websites,
and they have no obligation to keep their html stable so this could
break at any time lol.