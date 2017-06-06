"""
def test_parse():
    shell_script =
#!/bin/sh -e
#
# rc.local
#
# This script is executed at the end of each multiuser runlevel.
# Make sure that the script will "exit 0" on success or any other
# value on error.
#
# In order to enable or disable this script just change the execution
# bits.
#
# By default this script does nothing.
#exit 0
    c = std.io.Cursor.new(shell_script)
    result = parse(c)
    # println!("Result: :}", result)
    buff = []
    result2 = result.write(buff)
"""
