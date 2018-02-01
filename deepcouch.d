#!/usr/sbin/dtrace -s

#pragma D option destructive
#pragma D option quiet

/** On Mac OS, enable dtrace from recovery mode (cmd-R while rebooting) with
  * csrutil enable --without dtrace
  */

BEGIN
{
printf("%s: inserting delay of %d microseconds.\n", $$0, $1);
delay = $1 * 1000;
}

syscall::send*:entry
/execname == "memcached"/
{
chill(delay);
}
