#!/bin/bash
echo "==== $(TZ='Asia/Seoul' date '+%Y-%m-%d %H:%M:%S') ===="
cd /home/brighter87/Projects/assetmanager
/home/brighter87/Projects/assetmanager/venv/bin/python main.py
echo ""   # 빈 줄 하나 추가 (로그 가독성)

