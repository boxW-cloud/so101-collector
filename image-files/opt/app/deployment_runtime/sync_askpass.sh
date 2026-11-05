#!/bin/sh
# SSH 账号密码登录时，OpenSSH 通过 SSH_ASKPASS 调用本脚本取密码（collection_sync.py 设置）。
# 密码只来自只读挂载的密码文件，不出现在命令行、进程列表或日志里。
exec head -n 1 -- "$SO101_SYNC_PASSWORD_FILE"
