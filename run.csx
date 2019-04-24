#r "System.Configuration"
#r "System.Data"

using System;
using System.Net;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq; 

public class Schedule {
    public int seq;
    public string label;
    public string initialDate;
}

public static void Run(
    string item ,
    ICollector<string> replyQueue , 
    TraceWriter log
){

    log.Info( "Start Re Schedule" );

    string replyToken = null;
    {
        JObject parameter = JObject.Parse( item );
        replyToken = parameter[ "replyToken" ].ToString();
        log.Info( "Reply Token is " + replyToken );
    }

    string connectionString = 
        ConfigurationManager
        .ConnectionStrings[ "SQL_CONNECTION_STR" ]
        .ConnectionString;

    // 苗字の頭文字一覧を取得
    List<Schedule> schedules = new List<Schedule>();
    try {

        using( SqlConnection sqlConnection = new SqlConnection( connectionString ) ) {

            sqlConnection.Open();

            string query = @"
                SELECT
                    LIST.SEQ ,
                    LIST.LABEL ,
                    LIST.START_TIME ,
                    LIST.MINUTES_ABS ,
                    LIST.ABS_RANK
                FROM (
                    SELECT
                        CHILD_LIST.SEQ AS SEQ ,
                        FORMAT( CHILD_LIST.START_TIME , 'M/d HH:mm' ) + '~ ' + CHILD_LIST.NAME AS LABEL ,
                        FORMAT( CHILD_LIST.START_TIME , 'HH:mm' ) AS START_TIME ,
                        CHILD_LIST.MAIN_TIME ,
                        CHILD_LIST.MINUTES_ABS AS MINUTES_ABS ,
                        RANK() OVER( 
                            ORDER BY CHILD_LIST.MINUTES_ABS 
                        ) AS ABS_RANK
                    FROM ( 
                        SELECT
                            SCHEDULE_SEQ AS SEQ ,
                            START_TIME AS START_TIME ,
                            START_TIME AS MAIN_TIME ,
                            NAME AS NAME ,
                            ABS( 
                                DATENAME( DAY , START_TIME ) * 60 * 24
                                    + DATENAME( HOUR ,START_TIME ) * 60
                                    + DATENAME( MINUTE , START_TIME )
                                    - ( 
                                        DATENAME( DAY , GETDATE() ) * 60 * 24 
                                        + DATENAME( HOUR , GETDATE() ) * 60
                                        + DATENAME( MINUTE , GETDATE() )
                                    )
                            ) AS MINUTES_ABS 
                        FROM
                            T_SCHEDULE
                    ) AS CHILD_LIST
                ) AS LIST
                WHERE
                    LIST.ABS_RANK <= 30
                ORDER BY 
                    LIST.MAIN_TIME
            ";

            using( SqlCommand sqlCommand = new SqlCommand( query , sqlConnection ) ) {
                SqlDataReader reader = sqlCommand.ExecuteReader();
                while( reader.Read() ){
                    Schedule schedule = new Schedule();
                    schedule.seq = int.Parse( reader[0].ToString() );
                    schedule.label = reader[1].ToString();
                    schedule.initialDate = reader[2].ToString();
                    schedules.Add( schedule );
                }
            }

        }

    }
    catch( Exception ex ) {
        log.Error( "Select Schedule Labels Execute is Exception : " + ex );
        return;
    }

    JObject requestBody = new JObject();
    requestBody[ "replyToken" ] = replyToken;
    JArray messages = new JArray();
    JObject message = new JObject();
    message[ "type" ] = "template";
    message[ "altText" ] = "リスケ";
    JObject template = new JObject();
    template[ "type" ] = "carousel";
    
    int actionNumber;
    if( schedules.Count % 3 == 0 )
        actionNumber = 3;
    else if( schedules.Count % 2 == 0 )
        actionNumber = 2;
    else
        actionNumber = 1;

    JArray columns = new JArray();
    for( int templateNum = 0 ; templateNum < schedules.Count / actionNumber + ( schedules.Count % actionNumber == 0 ? 0 : 1 ) ; templateNum++ ){
        JObject column = new JObject();
        column[ "text" ] = "下記より変更するスケジュールを選択してください";
        JArray actions = new JArray();
        for( int actionNum = templateNum * actionNumber , count = 0 ; actionNum < schedules.Count && count < actionNumber ; actionNum++ , count++ ){
            JObject action = new JObject();
            action[ "type" ] = "datetimepicker";
            action[ "label" ] = ( schedules[ actionNum ].label.Length > 20 ) ? schedules[ actionNum ].label.Substring( 0 , 20 ) : schedules[ actionNum ].label;
            action[ "data" ] = "updateSchedule?seq=" + schedules[ actionNum ].seq;
            action[ "mode" ] = "time";
            action[ "initial" ] = schedules[ actionNum ].initialDate;
            actions.Add( action );
        }
        column[ "actions" ] = actions;
        columns.Add( column );
    }
    template[ "columns" ] = columns;
    message[ "template" ] = template;
    messages.Add( message );
    requestBody[ "messages" ] = messages;

    log.Info( "Request Body is " + requestBody.ToString() );
    replyQueue.Add( requestBody.ToString() );

    log.Info( "End Re Schedule" );

}
