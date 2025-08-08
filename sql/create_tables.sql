create table if not exists dwh_user (
    -- user_id int primary key,
    user_id int
    , sex varchar(10)
    , plan varchar(50)
    , age int
    , age_bin varchar(20)
    , ethnicity varchar(50)
    , department varchar(50)
    , nationality varchar(50)
    , country_of_work varchar(50)
);

create table if not exists dwh_event (
    -- event_id int primary key
    event_id int
    , event_name varchar(100)
    , num_slots int
    , event_registered_date varchar(20)
    , event_started_date varchar(20)
    , event_end_date varchar(20)
    , event_status varchar(20)
);

create table if not exists dwh_status (
    user_id int
    , user_status varchar(50)
    , created_date varchar(20)
    , updated_date varchar(20)
    -- foreign key (user_id) references user(user_id)
);

create table if not exists dwh_user_activity (
    user_id int
    , event_id int
    , user_status varchar(50)
    , status_updated_date varchar(20)
    -- foreign key (user_id) references user(user_id),
    -- foreign key (event_id) references event(event_id)
);

create table if not exists dwh_lifestyle_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , value int
    , unit varchar(20)
    -- foreign key (user_id) references user(user_id)
);

create table if not exists dwh_mindfulness_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , score int
    , score_band varchar(20)
    , mindfulness_band varchar(20)
    -- foreign key (user_id) references user(user_id)
);

create table if not exists dwh_health_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , value float
    , value_band varchar(20)
    , unit varchar(20)
    , health_index float
    -- foreign key (user_id) references user(user_id)
);


create table if not exists stg_user (
    -- user_id int primary key,
    user_id int
    , sex varchar(10)
    , plan varchar(50)
    , age int
    , age_bin varchar(20)
    , ethnicity varchar(50)
    , department varchar(50)
    , nationality varchar(50)
    , country_of_work varchar(50)
);

create table if not exists stg_event (
    -- event_id int primary key
    event_id int
    , event_name varchar(100)
    , num_slots int
    , event_registered_date varchar(20)
    , event_started_date varchar(20)
    , event_end_date varchar(20)
    , event_status varchar(20)
);

create table if not exists stg_user_status (
    user_id int
    , user_status varchar(50)
    , created_date varchar(20)
    , updated_date varchar(20)
);

create table if not exists stg_user_activity (
    user_id int
    , event_id int
    , user_status varchar(50)
    , status_updated_date varchar(20)
    -- foreign key (user_id) references user(user_id),
    -- foreign key (event_id) references event(event_id)
);

create table if not exists stg_lifestyle_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , value int
    , unit varchar(20)
    -- foreign key (user_id) references user(user_id)
);

create table if not exists stg_mindfulness_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , score int
    , score_band varchar(20)
    , mindfulness_band varchar(20)
    -- foreign key (user_id) references user(user_id)
);

create table if not exists stg_health_log (
    user_id int
    , log_type varchar(50)
    , log_date varchar(20)
    , value float
    , value_band varchar(20)
    , unit varchar(20)
    , health_index float
    -- foreign key (user_id) references user(user_id)
);

