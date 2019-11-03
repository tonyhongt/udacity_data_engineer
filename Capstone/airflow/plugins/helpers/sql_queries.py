class SqlQueries:
# CREATE STAGING TABLES

    staging_city_table_create = ("""
    create table if not exists staging_city
    (
    city varchar,
    state varchar,
    median_age float,
    male_population int,
    female_population int,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size float,
    state_code varchar,
    race varchar,
    count int
    );
    """)

    staging_immigration_table_create = ("""
    create table if not exists staging_immigration
    (
    knownid int,
    cicid float,
    i94yr float,
    i94mon float,
    i94cit float,
    i94res float,
    i94port varchar,
    arrdate float,
    i94mode float,
    i94addr varchar,
    depdate float,
    i94bir float,
    i94visa float,
    count float,
    dtadfile varchar,
    visapost varchar,
    occup varchar,
    entdepa varchar,
    entdepd varchar,
    entdepu varchar,
    matflag varchar,
    biryear float,
    dtaddto varchar,
    gender varchar,
    insnum int,
    airline varchar,
    admnum float,
    fltno varchar,
    visatype varchar
    );
    """)

    staging_statetemp_table_create = ("""
    create table if not exists staging_statetemp
    (
    date varchar,
    avgtemp float,
    avgtempuncert float,
    state varchar,
    country varchar
    );
    """)

    staging_reference_table_create = ("""
    create table if not exists staging_reference
    (
      filename varchar,
      id varchar,
      value varchar
    );
    """)


    # CREATE TARGET TABLES
    immigration_record_table_create = ("""
    create table if not exists immigration_record
    (
      cicid int,
      i94yr int sortkey distkey not null,
      i94mon int,
      i94cit int,
      i94res int,
      i94port varchar,
      arrdate date,
      depdate date,
      i94mode int,
      i94addr varchar,
      i94visa int,
      i94bir int,
      visapost varchar,
      biryear int,
      gender varchar,
      airline varchar,
      visatype varchar,
      primary key(cicid)
    );
    """)

    port_lookup_table_create = ("""
    create table if not exists port_lookup
    (
      port_code varchar sortkey,
      port_name varchar
    );
    """)

    country_lookup_table_create = ("""
    create table if not exists country_lookup
    (
      country_code int sortkey,
      country_name varchar
    );
    """)

    mode_lookup_table_create = ("""
    create table if not exists mode_lookup
    (
      mode_code int sortkey,
      mode_type varchar
    );
    """)

    visa_lookup_table_create = ("""
    create table if not exists visa_lookup
    (
      visa_code int sortkey,
      visa_category varchar
    );
    """)

    state_lookup_table_create = ("""
    create table if not exists state_lookup
    (
      state_code varchar sortkey,
      state_name varchar,
      male_pop int,
      female_pop int,
      total_pop int,
      total_veteran int,
      foreign_born int,
      total_hispanic int,
      total_white int,
      total_asian int,
      total_african int,
      total_native int,
      primary key (state_code)
    );
    """)

    state_temperature_table_create = ("""
    create table if not exists state_temperature
    (
      state_code varchar,
      year int sortkey distkey,
      month int,
      avgtemp float,
      avgtemp_uncert float,
      primary key(state_code, year, month)
    );
    """)

    # CLEAN DATA
    staging_immigration_table_dedup = ("""
    delete from
	    staging_immigration
    where 
	    cicid in (select cicid from staging_immigration group by cicid having count(*)>1);;
    """)    
    
    
    # INSERT DATA
    port_lookup_table_insert = ("""
    select 
        id,
        value
    from 
        staging_reference
    where 
        filename='i94prtl';
    """)

    country_lookup_table_insert = ("""
    select 
        id::int,
        value
    from 
        staging_reference
    where 
        filename='i94cntyl';
    """)

    mode_lookup_table_insert = ("""
    select 
        id::int,
        value
    from 
        staging_reference
    where 
        filename='i94model';
    """)

    visa_lookup_table_insert = ("""
    select 
        id::int,
        value
    from 
        staging_reference
    where 
        filename='i94visa';
    """)

    state_lookup_table_insert = ("""
    select 
        a.state_code,
        upper(a.state) as state_name,
        sum(a.male_population) as male_pop,
        sum(a.female_population) as female_pop,
        sum(a.total_population) as total_pop,
        sum(a.number_of_veterans) as total_veterans,
        sum(a.foreign_born) as foreign_born,
        sum(case when race='Hispanic or Latino' then count else 0 end) total_hispanic,
        sum(case when race='White' then count else 0 end) total_white,
        sum(case when race='Asian' then count else 0 end) total_asian,
        sum(case when race='Black or African-American' then count else 0 end) total_african,
        sum(case when race='American Indian and Alaska Native' then count else 0 end) total_native
    from 
        staging_city a
    group by
        a.state_code, a.state;
    """)    
    
    state_temperature_table_insert = ("""
    select
        state_cd state_code,
        extract(year from date::date),
        extract(month from date::date),
        avgtemp,
        avgtempuncert
    from 
        staging_statetemp a,
        (
         select id state_cd, value state 
         from staging_reference
         where filename='i94addrl'
        ) c
    where
        country='United States'
        and
        upper(a.state)=c.state
        and
        not exists (
                    select 1 
                    from state_temperature b 
                    where 
                        a.state=b.state_code
                    and
                        b.year=extract(year from a.date::date)
                    and
                        b.month=extract(month from a.date::date)
                    );
    """)

    immigration_record_table_insert = ("""
    select 
        cicid::int as cicid,
        i94yr::int as i94yr,
        i94mon::int as i94mon,
        i94cit::int as i94cit,
        i94res::int as i94res,
        i94port,
        '1960-01-01'::date + arrdate * interval '1 day' as arrdate,
        '1960-01-01'::date + depdate * interval '1 day' as depdate,
        i94mode::int as i94mode,
        i94addr,
        i94visa::int as i94visa,
        i94bir::int as i94bir,
        visapost,
        biryear::int as biryear,
        gender,
        airline,
        visatype
    from 
        staging_immigration a
    where
        not exists (select 1 from immigration_record b where a.cicid=b.cicid);
    """)
    
    create_staging_table_queries = [staging_city_table_create, staging_immigration_table_create, staging_statetemp_table_create, staging_reference_table_create]
    
    create_target_table_queries = [immigration_record_table_create, port_lookup_table_create, country_lookup_table_create, mode_lookup_table_create, visa_lookup_table_create, state_lookup_table_create, state_temperature_table_create]