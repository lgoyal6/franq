# demo.tape - franq's five self-checking scenarios, for the README.
#
# Setup and teardown live in their own scripts and run inside Hide blocks, so
# no build output or long command ever reaches a frame.
#
# The single visible command is real: it runs against a live franqd started on
# a scratch data directory, and each scenario asserts its own outcome and
# prints PASS, so a broken build cannot produce a passing recording. The port
# is 18180 rather than franqd's default, so recording this cannot write into a
# server already running on 1818.
#
# Run from the repo root:  vhs demo.tape

Output demo.gif

Set Shell "bash"
Set FontSize 15
Set Width 1180
Set Height 700
Set Theme "Dracula"
Set TypingSpeed 45ms
Set Padding 18

Hide
Type "./demo-setup.sh && clear" Enter
Sleep 20s
Show

Type "./bin/igor -addr http://127.0.0.1:18180 demo all" Sleep 400ms Enter

# The scenarios take real time: a 1.5s delayed job, a 1s lease expiry and a 3s
# starvation race. This waits for the work; it does not pad the recording.
Sleep 15s

Hide
Type "./demo-teardown.sh" Enter
Sleep 1s
