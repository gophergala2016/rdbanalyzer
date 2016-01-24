rdbanalyzer
===========

This is a tool to analyze your Redis RDB snapshot files. The goal is to output SVGs which help in analyzing what uses space in your Redis server.

It uses [rdbtools](https://github.com/vrischmann/rdbtools) and [svgo](https://github.com/ajstarks/svgo).

[Example report in SVG](https://vrischmann.me/upd/wXgkuser)

![](https://vrischmann.me/upd/bdC18RJ0)

how to run
----------

If you run the binary without arguments you'll get help, but here is the simplest way to run it: `rdbanalyzer -o report.svg mydump.rdb`. Beware that parsing can take quite some time if you have a big RDB file.

For example, on my i7 it takes approximately 2 minutes to parse a 4Gib RDB file.
