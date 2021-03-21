from pyspark import SparkConf, sql, SparkContext
from configparser import ConfigParser

def load_par(param, file):
    """

    :param param: Parameter from config file
    :param file: Name of file
    :return: path of file
    """
    parser = ConfigParser()
    parser.read('./cfg.ini')
    file_desc = parser.get(param, file)
    return file_desc

def gen_df (df_name, sqlContext, path_file):
    '''

    :param path_file: path of file to gen df
    :return: df
    '''

    df_gen = sqlContext\
        .read\
        .format('com.databricks.spark.csv')\
        .options(header='true', inferschema='true')\
        .load(path_file)

    df_gen.show()
    return df_gen


def export_file(df_cl, path_end):
        """

        :param df_final:
        :return:
        """
        df_cl \
        .coalesce(1)\
        .write \
        .format('com.databricks.spark.csv') \
        .options(header='true', inferschema='true') \
        .save(path_end)


def sql_statement(sqlContext, accounts, customers, d_time, d_month, d_year, transfer_ins, transfer_outs, pix_movements):
    transfer_ins.createOrReplaceTempView("transfer_ins")
    transfer_outs.createOrReplaceTempView("transfer_outs")
    d_time.createOrReplaceTempView("d_time")
    d_month.createOrReplaceTempView("d_month")
    d_year.createOrReplaceTempView("d_year")
    pix_movements.createOrReplaceTempView("pix_movements")
    customers.createOrReplaceTempView("customers")
    accounts.createOrReplaceTempView("accounts")
    trans_in = sqlContext.sql('''
                                 with no_pix_in as (
                                 SELECT  transfer_ins.amount,
                                         transfer_ins.account_id,
                                         customers.customer_id,
                                         transfer_ins.transaction_requested_at,
                                         transfer_ins.status,
                                         d_year.action_year year,
                                         d_month.action_month month 
                                  from transfer_ins 
                                  left join d_time on
                                    transfer_ins.transaction_requested_at = d_time.time_id
                                  left join d_month on
                                    d_month.month_id = d_time.month_id
                                  left join d_year on
                                    d_year.year_id = d_time.year_id
                                  LEFT JOIN accounts on
                                    accounts.account_id = transfer_ins.account_id
                                  LEFT JOIN customers on
                                    accounts.customer_id = customers.customer_id
                                  where transfer_ins.status == "completed"
                                        and d_year.action_year == "2020"
                                 ),
                                 no_pix_tot_in AS (
                                 SELECT SUM(amount) as total_in,
                                        customer_id,
                                        year,
                                        month 
                                 FROM no_pix_in
                                        group by customer_id, year, month 
                                 ),
                                 no_pix_out as (
                                 SELECT transfer_outs.amount,
                                         transfer_outs.account_id,
                                         customers.customer_id,
                                         transfer_outs.transaction_requested_at,
                                         transfer_outs.status,
                                         d_year.action_year year,
                                         d_month.action_month month 
                                 from transfer_outs 
                                 left join d_time on
                                    transfer_outs.transaction_requested_at = d_time.time_id
                                 left join d_month on
                                    d_month.month_id = d_time.month_id
                                 left join d_year on
                                    d_year.year_id = d_time.year_id
                                 LEFT JOIN accounts on
                                    accounts.account_id = transfer_outs.account_id
                                 LEFT JOIN customers on
                                    accounts.customer_id = customers.customer_id
                                 where transfer_outs.status == "completed"
                                       and d_year.action_year == "2020"
                                 ),
                                 no_pix_tot_out AS (
                                     SELECT SUM(amount) as total_out,
                                            customer_id,
                                            year,
                                            month 
                                     FROM no_pix_out
                                            group by customer_id, year, month 
                                 ),
                                pix_clean AS (
                                SELECT pix_movements.pix_amount,
                                       pix_movements.account_id,
                                       customers.customer_id, 
                                       pix_movements.in_or_out,
                                       pix_movements.pix_completed_at,
                                       pix_movements.status,
                                       d_year.action_year year,
                                       d_month.action_month month 
                                FROM pix_movements
                                LEFT JOIN d_time on
                                    pix_movements.pix_completed_at = d_time.time_id
                                LEFT JOIN d_month on
                                    d_month.month_id = d_time.month_id
                                LEFT JOIN d_year on
                                    d_year.year_id = d_time.year_id
                                LEFT JOIN accounts on
                                    accounts.account_id = pix_movements.account_id
                                LEFT JOIN customers on
                                    accounts.customer_id = customers.customer_id
                                WHERE pix_movements.status == "completed"
                                      AND d_year.action_year == "2020"
                                ), 
                                pix_in_tot AS ( 
                                    SELECT 
                                        SUM(pix_amount) as total_in,
                                        customer_id,
                                        year,
                                        month 
                                 FROM pix_clean
                                 WHERE pix_clean.in_or_out = "pix_in"
                                 GROUP BY customer_id, year, month 
                                ),
                                pix_out_tot AS ( 
                                    SELECT 
                                        SUM(pix_amount) as total_out,
                                        customer_id,
                                        year,
                                        month 
                                 FROM pix_clean
                                 WHERE pix_clean.in_or_out = "pix_out" 
                                 GROUP BY customer_id, year, month
                                ),
                                total_in_union AS (
                                SELECT * 
                                    FROM pix_in_tot
                                UNION ALL
                                SELECT * 
                                    FROM no_pix_tot_in
                                ),
                                total_out_union AS (
                                SELECT * 
                                    FROM pix_out_tot
                                UNION ALL
                                SELECT * 
                                    FROM no_pix_tot_out
                                ),
                                total_in AS (
                                SELECT
                                    SUM(total_in) as total_in,
                                    customer_id,
                                    year,
                                    month
                                FROM total_in_union
                                GROUP BY customer_id,
                                         year,
                                         month
                                ),
                                total_out AS (
                                SELECT
                                    SUM(total_out) as total_out,
                                    customer_id,
                                    year,
                                    month
                                FROM total_out_union
                                GROUP BY customer_id,
                                         year,
                                         month
                                order by customer_id,
                                         year,
                                         month
                                ),
                                acc_month_balance AS (
                                SELECT
                                    total_in.year year,
                                    total_in.month month,
                                    total_in.customer_id customer_id,
                                    customers.first_name first_name,
                                    ifnull(total_in.total_in, 0.0) total_transfer_in,
                                    ifnull(total_out.total_out, 0.0) total_transfer_out,
                                    (ifnull(total_in.total_in, 0.0) - ifnull(total_out.total_out, 0.0)) account_monthly_balance
                                    FROM total_in
                                    LEFT JOIN total_out on
                                        total_in.customer_id = total_out.customer_id
                                        AND total_in.year = total_out.year
                                        AND total_in.month = total_out.month
                                    LEFT JOIN customers on
                                        total_in.customer_id = customers.customer_id
                                    ORDER BY total_in.customer_id, total_in.year, total_in.month
                                )
                                select * from acc_month_balance
                                        ''')
    trans_in.show()
    return trans_in

if __name__ == '__main__':
    conf = SparkConf().setAppName("Prepare CSV")
    sc = SparkContext(conf=conf)
    sqlContext = sql.SQLContext(sc)

    acc_path = load_par('CSV', 'accounts')
    accounts = gen_df('accounts', sqlContext, acc_path)

    #city_path = load_par('CSV', 'city')
    #city = gen_df('city', sqlContext, city_path)

    #country_path = load_par('CSV', 'country')
    #country = gen_df('country', sqlContext, country_path)

    customers_path = load_par('CSV', 'customers')
    customers = gen_df('customers', sqlContext, customers_path)

    d_month_path = load_par('CSV', 'd_month')
    d_month = gen_df('d_month', sqlContext, d_month_path)

    d_time_path = load_par('CSV', 'd_time')
    d_time = gen_df('d_time', sqlContext, d_time_path)

    d_year_path = load_par('CSV', 'd_year')
    d_year = gen_df('d_year', sqlContext, d_year_path)

    pix_movements_path = load_par('CSV', 'pix_movements')
    pix_movements = gen_df('pix_movements', sqlContext, pix_movements_path)

    transfer_ins_path = load_par('CSV', 'transfer_ins')
    transfer_ins = gen_df('transfer_ins', sqlContext, transfer_ins_path)

    transfer_outs_path = load_par('CSV', 'transfer_outs')
    transfer_outs = gen_df('transfer_outs', sqlContext, transfer_outs_path)

    df_final = sql_statement(sqlContext, accounts, customers, d_time, d_month, d_year, transfer_ins, transfer_outs, pix_movements)
    transfer_outs_path = load_par('END', 'path_csv')
    export_file(df_final, transfer_outs_path)
