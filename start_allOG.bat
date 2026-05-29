@echo off
echo.
echo Starting all ClickSend trading modules...
echo.

:: First module
start "ClickSend AT2 - livealerts (app.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\ClickSend_AT2-1RR - livealerts" && python app.py"

:: Second module  
start "ClickSend AT2 - livealert (short_locate1.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\ClickSend_AT2-1RR - livealerts" && python short_locate1.py"

:: Third module
start "ClickSend AT2 - livealert (tms_sale.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\ClickSend_AT2-1RR - livealerts" && python tms_sale.py"

:: Fourth module
start "ClickSend SPara (short_locate1.py)" ^
  cmd /c "cd /d "C:\Users\a1031\Documents\Trading apps\clicksend-SellOpen\ClickSend_SPara" && python short_locate1.py"

echo.
echo All 4 modules launched successfully!
timeout /t 4 >nul
exit