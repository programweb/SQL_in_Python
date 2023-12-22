#!/usr/bin/python
######################################################
# api/classes/metadata_statistics_class.py
# purpose: calculations to summarize metadata vs draft
######################################################

from classes.meta_stats_out_class import meta_stats_out

class metadata_statistics_helper:

    def count_metadata_adds(draft_id, hierarchy_name):

        sql = """
            SELECT COUNT(*) AS adds FROM
            (
            """ + metadata_statistics_helper.sql_snippet_draft_union() + """
            EXCEPT
            (
            """ + metadata_statistics_helper.sql_snippet_metadata_history() + """    
            )
            ) AS totals
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        return meta_stats_out.select_one_to_object (sql, params)

    def count_metadata_deletes(draft_id, hierarchy_name):

        sql = """
            SELECT COUNT(*) AS deletes FROM
            (
            """ + metadata_statistics_helper.sql_snippet_metadata_history() + """    
            EXCEPT
            (
            """ + metadata_statistics_helper.sql_snippet_draft_union() + """
            )
            ) AS totals
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        return meta_stats_out.select_one_to_object (sql, params)

    def count_metadata_draft_deletes_verbose(draft_id, hierarchy_name):

        sql = """
            SELECT COUNT(*) FROM
            -- detail data available by removing preceding COUNT wrapper ---
            (
            SELECT
                d.draft_id,
                smh.HIERARCHY_id, 
                smh.HIERARCHY_metadata_type_id,
                smh.HIERARCHY_metadata_value
            FROM
                DB-NAME.HIERARCHY_metadata_history smh
            INNER JOIN
                DB-NAME.draft d ON d.hierarchy_seed_version_id = smh.HIERARCHY_metadata_version_id     
            WHERE
                d.draft_id = :draft_id
            -- remove any matching draft data ---
            EXCEPT
            (
            -- removal combines metadata draft ---
            SELECT
                dmh.draft_id,
                dmh.angel_id,
                dmh.hierarchy_metadata_type_id,
                dmh.hierarchy_metadata_value
            FROM
                DB-NAME.draft_metadata_history dmh
            WHERE
                dmh.draft_id = :draft_id
            -- removal combines draft_item data ---
            UNION
            SELECT
                di.draft_id,
                di.angel_id, 
                col.metadata_type_id,
                di.value
            FROM
                DB-NAME.draft_item di
            INNER JOIN
                DB-NAME.draft_column col 
                ON col.draft_column_id = di.draft_column_id
            WHERE
                di.draft_id = :draft_id
            )
            ) AS totals
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        return meta_stats_out.select_to_object (sql, params)

    def get_disposition(draft_id):
        sql = """
            SELECT 
                disposition
            FROM
                DB-NAME.draft
            WHERE
                draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        result = meta_stats_out.select_one_to_object (sql, params)
        disposition = result['data']['disposition']
        return disposition

    def get_draft_item_summary(draft_id):
        sql = """
            SELECT 
                COUNT(*) AS draft_item_population
            FROM
                DB-NAME.draft_item di
            WHERE
                di.draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        result = meta_stats_out.select_one_to_object (sql, params)
        return result

    def get_draft_metadata_history_summary(draft_id):
        sql = """
            SELECT 
                COUNT(*) AS draft_metadata_history_population
            FROM
                DB-NAME.draft_metadata_history dmh
            WHERE
                dmh.draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        result = meta_stats_out.select_one_to_object (sql, params)
        return result

    def get_hierarchy_name(draft_id):
        sql = """
            SELECT 
                hierarchy_name
            FROM
                DB-NAME.draft d
            WHERE
                d.draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        result = meta_stats_out.select_one_to_object (sql, params)
        hierarchy_name = result['data']['hierarchy_name']
        return hierarchy_name

    def get_import_description(draft_id):
        # select_one_to_object is not happy if there are no rows (no spreadsheet import);
        # so, test IF there are any rows first
        # if not, return 'no_rows' then select_one_to_object will be happy no matter what :-)
        sql = """
            SELECT IF (
                -- if there are rows
                (SELECT COUNT(*) FROM DB-NAME.draft_log WHERE draft_id = :draft_id AND `action` = 'import metadata') > 0,
                -- then get the latest logged description of the metadata import (spreadsheet upload)
                (   SELECT
                        COALESCE(description, '')
                    FROM
                        DB-NAME.draft_log
                    WHERE
                        draft_id = :draft_id
                    AND
                        `action` = 'import metadata'
                    ORDER BY
                        modify_date DESC
                    LIMIT 1),
                -- else say 'no_rows' (can make this more human-readable in the model)
                'no_rows')  AS description
            FROM DB-NAME.draft_log LIMIT 1
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        return meta_stats_out.select_one_to_object(sql, params)

    def get_metadata_summary(draft_id, hierarchy_name):

        sql = """
            SELECT 
                d.hierarchy_seed_version_id AS metadata_version_id,
                COUNT(*) AS metadata_population
            FROM
                DB-NAME.draft d
            INNER JOIN
                DB-NAME.HIERARCHY_metadata_history smh
                ON smh.HIERARCHY_metadata_version_id = d.hierarchy_seed_version_id   
            WHERE
                d.draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        result = meta_stats_out.select_one_to_object (sql, params)
        return result

    def get_modified_count(draft_id, hierarchy_name):

        sql = """
            SELECT
                COUNT(*) AS modified_count
            FROM
                DB-NAME.draft_metadata_history dmh
            INNER JOIN
                DB-NAME.draft d
                ON d.draft_id = dmh.draft_id
            INNER JOIN
                HIERARCHY_metadata_history smh
                ON smh.HIERARCHY_metadata_type_id =  dmh.hierarchy_metadata_type_id
                AND smh.HIERARCHY_id = dmh.angel_id
            INNER JOIN
                DB-NAME.draft_column dc
                ON dc.metadata_type_id = dmh.hierarchy_metadata_type_id
                AND dc.hierarchy_table = '{}_metadata_history'
            LEFT OUTER JOIN DB-NAME.draft_item di
                ON di.draft_id = dmh.draft_id
                AND di.angel_id = dmh.angel_id
                AND di.draft_column_id = dc.draft_column_id
            WHERE
                dmh.draft_id = :draft_id
            AND
                smh.HIERARCHY_metadata_version_id = d.hierarchy_seed_version_id
            AND
                dmh.angel_id = smh.HIERARCHY_id
            AND
                COALESCE(di.value, dmh.hierarchy_metadata_value) <> smh.HIERARCHY_metadata_value
        """.format(hierarchy_name)

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        result = meta_stats_out.select_one_to_object (sql, params)
        return result

    def get_seeded_count(draft_id, hierarchy_name):
        sql = """
            SELECT COUNT(*) AS seeded_population
            FROM HIERARCHY_metadata_history
            WHERE HIERARCHY_metadata_version_id = (SELECT hierarchy_seed_version_id FROM DB-NAME.draft WHERE draft_id = :draft_id);
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        result = meta_stats_out.select_one_to_object (sql, params)
        return result

    def get_statistics_for_draft(draft_id):

        hierarchy_name = metadata_statistics_helper.get_hierarchy_name(draft_id)

        result = metadata_statistics_helper.count_metadata_deletes(draft_id, hierarchy_name)
        deletes_count = result['data']['deletes']

        result = metadata_statistics_helper.count_metadata_adds(draft_id, hierarchy_name)
        adds_count = result['data']['adds']

        result = metadata_statistics_helper.get_metadata_summary(draft_id, hierarchy_name)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)

        metadata_version_id = result['data']['metadata_version_id']    
        metadata_population = result['data']['metadata_population']    

        result = metadata_statistics_helper.get_draft_metadata_history_summary(draft_id)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        draft_metadata_history_population = result['data']['draft_metadata_history_population']    

        result = metadata_statistics_helper.get_draft_item_summary(draft_id)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        draft_item_population = result['data']['draft_item_population']    

        is_metadata = metadata_statistics_helper.is_metadata(draft_id)

        disposition = metadata_statistics_helper.get_disposition(draft_id)

        result = metadata_statistics_helper.get_seeded_count(draft_id, hierarchy_name)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        seeded_population = result['data']['seeded_population']

        result = metadata_statistics_helper.get_total_population(draft_id, hierarchy_name)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        total_population = result['data']['total_population']

        result = metadata_statistics_helper.get_import_description(draft_id)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        import_metadata_description = result['data']['description']

        result = metadata_statistics_helper.get_modified_count(draft_id, hierarchy_name)
        if result['result'] != 'OK':
            return meta_stats_out.error_to_object(result)
        modified_count = result['data']['modified_count'] 
    

        #--assemble response----------------
        data = {
            "draft_id": draft_id,
            "is_metadata": is_metadata,
            "disposition": disposition,
            "draft_item_population": draft_item_population,
            "draft_metadata_history_population": draft_metadata_history_population,
            "metadata_version_id": metadata_version_id,
            "metadata_population": metadata_population,
            "import_metadata_description": import_metadata_description,
            "total_deletes": deletes_count,
            "total_adds": adds_count,
            "total_modifieds": modified_count,
            "total_population": total_population,
            "seeded_population": seeded_population,
        }

        response = {
            "result": "OK",
            "data": data,
            "row_count": 1,
        }

        return (response)

    def get_total_population(draft_id, hierarchy_name):

        sql = """
            SELECT COUNT(*) AS total_population
            FROM
                (
                """ + metadata_statistics_helper.sql_snippet_draft_union() + """
                ) unused_but_required_alias;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql, hierarchy_name)
        return meta_stats_out.select_one_to_object (sql, params)

    def is_metadata(draft_id):
        sql = """
            SELECT 
                COALESCE(is_metadata,0) AS is_metadata
            FROM
                DB-NAME.draft
            WHERE
                draft_id = :draft_id
            ;    
        """

        params = {
            'draft_id': draft_id,
        }

        sql = meta_stats_out.hierarchy_abstractor(sql)
        result = meta_stats_out.select_one_to_object (sql, params)
        is_metadata_num = result['data']['is_metadata']
        return bool(int(is_metadata_num))

    def sql_snippet_draft_union(include_value = False):

        dmh_value_sql = ''
        di_value_sql = ''
        if include_value:
            dmh_value_sql = ', dmh.hierarchy_metadata_value '
            di_value_sql = ', di.value '

        sql = """
            SELECT
                dmh.draft_id,
                dmh.angel_id,
                dmh.hierarchy_metadata_type_id
            """ + dmh_value_sql + """
            FROM
                DB-NAME.draft_metadata_history dmh
            WHERE
                dmh.draft_id = :draft_id
            -- removal combines draft_item data ---
            UNION
            SELECT
                di.draft_id,
                di.angel_id, 
                col.metadata_type_id
            """ + di_value_sql + """
            FROM
                DB-NAME.draft_item di
            INNER JOIN
                DB-NAME.draft_column col 
                ON col.draft_column_id = di.draft_column_id
            WHERE
                di.draft_id = :draft_id
        """

        return sql

    def sql_snippet_metadata_history(include_value = False):

        value_sql = ''
        if include_value:
            value_sql = ', smh.HIERARCHY_metadata_value'

        sql = """
            SELECT
                d.draft_id,
                smh.HIERARCHY_id, 
                smh.HIERARCHY_metadata_type_id
            """ + value_sql + """
            FROM
                DB-NAME.HIERARCHY_metadata_history smh
            INNER JOIN
                DB-NAME.draft d ON d.hierarchy_seed_version_id = smh.HIERARCHY_metadata_version_id     
            WHERE
                d.draft_id = :draft_id
        """

        return sql