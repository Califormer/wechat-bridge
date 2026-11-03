# Hourly watchdog cron (example template)
#
# This is a template for the hourly safety-net check that backs up the
# 5-second hook poll. It is NOT a live cron definition: fill in the
# placeholders below for your own scheduler (Muse cron, systemd timer,
# or any other scheduler that can run a command hourly).
#
# Command (hourly, silent when healthy):
#   cd /home/USERNAME/workspace/wechat-bridge && python3 ops/watchdog.py
#
# Behaviour:
# - No output: everything is healthy, stop here.
# - One JSON line: follow its `alert` field. If the alert is
#   `recovery_failed`, you may run `ops/repair.sh`. If it is `auth` or
#   `credentials_missing`, the WeChat login may have expired: re-scan
#   the QR code with `bridge.py login`, do not generate a QR yourself.
# - Only if the WeChat channel itself is down (so send-direct cannot
#   deliver) should the alert be reported in another channel.
