class AirflowConnIds:
    S3 = 'aws_conn'
    REDSHIFT = 'capstoneuser'


class S3Buckets:
    CAPSTONE = 'us-immigration'


class General:
    SCHEMA = 'public'
    CSV_TABLES = ["airport_codes", "port_of_entry_codes", "nationality_codes",
                  "port_of_issue_codes", "visa_codes",
                  "us_cities_demographics",
                  "i94cit_i94res", "i94port", "i94mode", "i94addr", "i94visa"]
    PARQUET_TABLES = ["immigration"]
    TABLES = CSV_TABLES + PARQUET_TABLES


class SQLQueries:
    DROP_TABLE = """
        DROP TABLE IF EXISTS {schema}.{table}
    """ # noqa

    COPY_CSV_TABLE = """
        COPY {schema}.{table} FROM '{s3_uri}'
        CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
        IGNOREHEADER 1
        COMPUPDATE OFF
        TRUNCATECOLUMNS
        CSV;
    """ # noqa

    COPY_PARQUET_TABLE = """
        COPY {schema}.{table} FROM '{s3_uri}'
        IAM_ROLE '{aws_iam_role}'
        FORMAT AS PARQUET;
    """ # noqa

    INCREMENTAL_APPEND = """
        ALTER TABLE {schema}.{table} APPEND FROM {schema}.{staged_table} FILLTARGET;
    """ # noqa

    GRANT_USAGE = """
        GRANT USAGE ON SCHEMA {schema} TO {redshift_user};
    """ # noqa

    GRANT_SELECT = """
        GRANT SELECT ON {schema}.{table} TO {redshift_user};
    """ # noqa

    CREATE = {}
    CREATE['immigration'] = """
    	CREATE TABLE IF NOT EXISTS public.immigration (
    		cicid FLOAT,
            i94yr FLOAT,
            i94mon FLOAT,
            i94cit FLOAT,
            i94res FLOAT,
            i94port VARCHAR,
            arrdate FLOAT,
            i94mode FLOAT,
            i94addr VARCHAR,
            depdate FLOAT,
            i94bir FLOAT,
            i94visa FLOAT,
            count FLOAT,
            dtadfile VARCHAR,
            visapost VARCHAR,
            occup VARCHAR,
            entdepa VARCHAR,
            entdepd VARCHAR,
            entdepu VARCHAR,
            matflag VARCHAR,
            biryear FLOAT,
            dtaddto VARCHAR,
            gender VARCHAR,
            insnum VARCHAR,
            airline VARCHAR,
            admnum FLOAT,
            fltno VARCHAR,
            visatype VARCHAR
        );
    """ # noqa

    CREATE['airport_codes'] = """
    	CREATE TABLE IF NOT EXISTS public.airport_codes (
    		ident VARCHAR,
    		type VARCHAR,
    		name VARCHAR,
    		elevation_ft FLOAT,
    		continent VARCHAR,
    		iso_country VARCHAR,
    		iso_region VARCHAR,
    		municipality VARCHAR,
    		gps_code VARCHAR,
    		iata_code VARCHAR,
    		local_code VARCHAR,
    		coordinates VARCHAR,
    		lat FLOAT,
    		long FLOAT
        );
    """ # noqa

    CREATE['port_of_entry_codes'] = """
    	CREATE TABLE IF NOT EXISTS public.port_of_entry_codes (
    		code VARCHAR,
    		location VARCHAR,
    		city VARCHAR,
    		state_or_country VARCHAR
        );
    """ # noqa

    CREATE['port_of_issue_codes'] = """
    	CREATE TABLE IF NOT EXISTS public.port_of_issue_codes (
    		port_of_issue VARCHAR,
    		code VARCHAR
        );
    """ # noqa
    CREATE['visa_codes'] = """
    	CREATE TABLE IF NOT EXISTS public.visa_codes (
    		class_of_admission VARCHAR,
    		ins_status_code VARCHAR,
    		description VARCHAR,
    		section_of_law VARCHAR
        );
    """ # noqa

    CREATE['nationality_codes'] = """
    	CREATE TABLE IF NOT EXISTS public.nationality_codes (
    		nationality VARCHAR,
    		code VARCHAR
        );
    """ # noqa

    CREATE['us_cities_demographics'] = """
    	CREATE TABLE IF NOT EXISTS public.us_cities_demographics (
    		city VARCHAR,
    		state VARCHAR,
    		median_age FLOAT,
    		male_population FLOAT,
    		female_population FLOAT,
    		total_population FLOAT,
    		number_of_veterans FLOAT,
    		foreign_born FLOAT,
    		average_household_size FLOAT,
    		state_code VARCHAR,
    		race VARCHAR,
    		count INT
        );
    """ # noqa

    CREATE['i94cit_i94res'] = """
    	CREATE TABLE IF NOT EXISTS public.i94cit_i94res (
    		code INT,
    		country VARCHAR
        );
    """ # noqa

    CREATE['i94port'] = """
    	CREATE TABLE IF NOT EXISTS public.i94port (
    		code VARCHAR,
    		port_of_entry VARCHAR,
    		city VARCHAR,
    		state_or_country VARCHAR
        );
    """ # noqa

    CREATE['i94mode'] = """
    	CREATE TABLE IF NOT EXISTS public.i94mode (
    		code INT,
    		transportation VARCHAR
        );
    """ # noqa

    CREATE['i94addr'] = """
    	CREATE TABLE IF NOT EXISTS public.i94addr (
    		code VARCHAR,
    		state VARCHAR
        );
    """ # noqa

    CREATE['i94visa'] = """
    	CREATE TABLE IF NOT EXISTS public.i94visa (
    		code INT,
    		reason_for_travel VARCHAR
        );
    """ # noqa
