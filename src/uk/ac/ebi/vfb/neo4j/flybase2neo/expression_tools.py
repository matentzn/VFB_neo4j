from .fb_tools import FB2Neo

def expand_stage_range(nc, start, end):
    """nc = neo4j_connect object
    start = start stage (short_form_id string)
    end = end stage (short_form_id string)
    Returns list of intermediate stages.
    """
    stages = [start, end]
    statements = [
        'MATCH p=shortestPath((s:FBDV {short_form:"%s"})<-[:immediately_preceded_by*]-" \
        "(e:FBDV {short_form:"%s"})) RETURN extract(x IN nodes(p) | x.short_form)' % (start, end)]
    r = nc.commit_list(statements)
    stages.append(r[0]['data'][0]['row'][0])
    return stages

class ExpressionWriter(FB2Neo):

    def get_all_expression(self, limit=False):
        query = 'SELECT c.name as cvt, db.name as cvt_db, dbx.accession as cvt_acc, ec.rank as ec_rank, ' \
                't1.name as ec_type, ectp.value as ectp_value, ' \
                't2.name as ectp_name, ectp.rank as ectp_rank, ' \
                'e.uniquename as fbex ' \
                'FROM expression_cvterm ec ' \
                'JOIN expression e on ec.expression_id=e.expression_id ' \
                'LEFT OUTER JOIN expression_cvtermprop ectp on ec.expression_cvterm_id=ectp.expression_cvterm_id  ' \
                'JOIN cvterm c on ec.cvterm_id=c.cvterm_id  ' \
                'JOIN dbxref dbx ON (dbx.dbxref_id = c.dbxref_id) ' \
                'JOIN db ON (dbx.db_id=db.db_id) ' \
                'JOIN cvterm t1 on ec.cvterm_type_id=t1.cvterm_id  ' \
                'LEFT OUTER JOIN cvterm t2 on ectp.type_id=t2.cvterm_id'

        if limit:
            query += " limit %d" % limit

#         cvt         |      cvt_db      |                cvt_acc                 | ec_rank | ec_type | ectp_value | ectp_name | ectp_rank |    fbex
# --------------------+------------------+----------------------------------------+---------+---------+------------+-----------+-----------+-------------
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000001
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000001
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000001
#  70-100% egg length | FBcv             | 0000132                                |       1 | anatomy |            | qualifier |         0 | FBex0000001
#  embryonic stage 4  | FBdv             | 00005306                               |       0 | stage   |            |           |           | FBex0000002
#  immunolocalization | FlyBase_internal | experimental assays:immunolocalization |       0 | assay   |            |           |           | FBex0000002
#  organism           | FBbt             | 00000001                               |       0 | anatomy |            |           |           | FBex0000002
#  90-100% egg length | FBcv             | 0000139                                |       1 | anatomy |            | qualifier |         0 | FBex0000002
#  embryonic stage 1  | FBdv             | 00005291                               |       0 | stage   | FROM       | operator  |         0 | FBex0000003
#  embryonic stage 5  | FBdv             | 00005311                               |       1 | stage   | TO         | operator  |         0 | FBex0000003

        exp = self.query_fb(query)

        # make dict keyed on FBex : TAP-like structure
        FBex_lookup = {}

        for d in exp:
            FBex_lookup[d['fbex']] = {}
            FBex_lookup[d['fbex']][d['ec_type']] = {}
            if 'stage' in d['ec_type']:
                FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']] = {}
                FBex_lookup[d['fbex']][d['ec_type']][d['ectp_value']].update(
                        {"short_form": d['cvt_db'] + '_' + d['cvf_acc'],
                         "label": d['cvt'], 'rank1': d['ec_rank'],
                         'rank2': d['ectp_rank']})
            elif 'anatomy' in d['ec_type']:
                if 'qualifier' in d['ectp_name']:
                    FBex_lookup[d['fbex']][d['ec_type']][d['ectp_name']] = {}
                    FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank'],
                                 'rank2': d['ectp_rank']})
                else:
                    FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank']})
            elif 'assay' in d['ec_type']:
                FBex_lookup[d['fbex']][d['ec_type']].update(
                                {'short_form': d['cvt_db'] + '_' + d['cvf_acc'],
                                 'label': d['cvt'],
                                 'rank1': d['ec_rank']})

        self.FBex_lookup = FBex_lookup

    def write_expression(self, pub, expression_pattern, FBex):


        # Phase 1 Generate intermediate (stage restricted) anatomy nodes
        # Phase 2



        ### Where do the different lines get merged?  Do we make a intermediate data structure, or do it all in cypher?
        ### Given that these are already sorted on FBex, couldn't this be done within the loop structure?

        ### Schema for EP
        # https://github.com/VirtualFlyBrain/VFB_neo4j/issues/2
        # (as:Class:Anatomy { "label" :  'lateral horn  - from S-x to S-y', short_form : 'FBex...', assay: ''})
        # (as)-[SubClassOf]->(:Anatomy { label:  'lateral horn', short_form: "FBbt_...." })
        # (as)-[during]->(sr:stage { label: 'stage x to y'} )
        # (sr)-[start]->(:stage { label: 'stage x', short_form: 'FBdv_12345678' }
        # (sr)-[end]->(:stage { label: 'stage y', short_form: 'FBdv_22345678' }




        return