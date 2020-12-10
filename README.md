# IQfeed
Connects to IQ Feed local gateway, collects and stores interval bars in a database
Requires a subscription to IQ Feed to work
Uses Postresql database
Custom logger class has option to send message via Telegram
Use of Telegram messaging requires an account and a "bot" set up within Telegram

Need a file named "user.pwd" with the following (do not include quotations):

[iqfeed]
productID =
iq_user = 
iq_pass = 

[telegram]
chatID = 
botToken = 

[postgres]
db_user = 
db_pass = 