@echo off
echo.
echo Starting all ClickSend trading modules...
echo.

:: First module
start "ClickSend AT2 - livealerts (app.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\Trading_bot" && python app.py"

:: Second module  
start "ClickSend AT2 - livealert (short_locate1.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\Trading_bot" && python short_locate1.py"

:: Third module
start "ClickSend AT2 - livealert (tms_sale.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\Trading_bot" && python tms_sale.py"


echo.
echo All 3 modules launched successfully!
timeout /t 4 >nul
exit