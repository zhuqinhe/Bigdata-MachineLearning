#!/bin/sh

Force=$1

NE_HOME="/opt/fonsview/NE/daas/fsrtap/flink-ott-user-online-stat"
NE_ETC_HOME="$NE_HOME/etc"
NE_BIN_HOME="$NE_HOME/bin"
NE_LOG_HOME="$NE_HOME/logs"
APP_NAME="flink-ott-user-online-stat";

if [ ! -d $NE_ETC_HOME ];then	
		mkdir	$NE_ETC_HOME -p	
		echo "directory $NE_ETC_HOME not exist,now create it"
fi		

if [ ! -d $NE_BIN_HOME ];then	
		mkdir	$NE_BIN_HOME -p	
		echo "directory $NE_BIN_HOME not exist,now create it"
fi	

if [ ! -d $NE_LOG_HOME ];then	
		mkdir	$NE_LOG_HOME -p	
		echo "directory $NE_LOG_HOME not exist,now create it"
fi

chmod -R 755 $NE_HOME 


##########INIT Directory##########
function intDirectory()
{	
	if [ ! -d "$1" ];then
		mkdir "$1" -p
		echo "directory $1 not exist,now create it"
	fi		
}


##########Copy file from install package to target directory##########
#overwrite install
if [ "$Force" == "-f" ]; then
		\cp -rf ../etc/* $NE_ETC_HOME
		echo "Copy etc file to $NE_ETC_HOME"	
		
fi

\cp -rf ../bin/* $NE_BIN_HOME
\cp -rf ../etc/* $NE_ETC_HOME
chmod +x $NE_BIN_HOME/*.sh
echo "Copy bin file to $NE_BIN_HOME"	

##########Copy *.jar from install package to plugin.d directory##########

\cp -rf ../flink-ott-user-online-stat.jar $NE_HOME/flink-ott-user-online-stat.jar
echo "Copy flink-ott-user-online-stat.jar to $NE_HOME"


echo "add crontab..."

#######add crontab###########
crontab_add(){
cron_path="$1"
cat >/etc/cron.d/$cron_path<<EOF
# Run service application     
*/5 * * * * root  $NE_HOME/bin/check.sh
EOF
}

#########check crontab###########
check_crontab (){
    if [ ! -f  /etc/cron.d/$APP_NAME ]
      then
          crontab_add  "$APP_NAME"           
     else
     	rm -rf /etc/cron.d/$APP_NAME
     	echo "File already exists!"
     	crontab_add  "$APP_NAME"
    fi   
}

check_crontab "$APP_NAME"
echo -e "crontab add success!" 

echo "install success..."
exit 0