from .fb_tools import FB2Neo

def clean_sgml_tags(sgml_string):
    sgml_string = re.sub('<up>', '[', sgml_string)
    sgml_string = re.sub('</up>', ']', sgml_string)
    sgml_string = re.sub('<down>', '[[', sgml_string)
    sgml_string = re.sub("</down>", ']]', sgml_string)
    return sgml_string

def map_feature_type(fbid, ftype):
    mapping = {'transgenic_transposon': 'SO_0000796',
               'insertion_site': 'SO_0001218',
                'transposable_element_insertion_site': 'SO_0001218',
                'natural_transposon_isolate_named': 'SO_0000797',
                'chromosome_structure_variation': 'SO_1000183'
                }
    if ftype == 'gene':
        if re.match('FBal', fbid):
            return 'SO_0001023'
        else:
            return 'SO_0000704 '
    elif ftype in mapping.keys():
        return mapping[ftype]
    else:
        return 'SO_0000110' # Sequence feature


class FeatureMover(FB2Neo):

    def name_synonym_lookup(self, fbids):
        """Makes unicode name primary.  Makes everything else a synonym"""
        # stypes: symbol nickname synonym fullname
        query = "SELECT f.uniquename as fbid, s.name as ascii_name, " \
                "stype.name AS stype, " \
                "fs.is_current, s.synonym_sgml as unicode_name " \
                "FROM feature f " \
                "JOIN feature_synonym fs on (f.feature_id=fs.feature_id) " \
                "JOIN synonym s on (fs.synonym_id=s.synonym_id) " \
                "JOIN cvterm stype on (s.type_id=stype.cvterm_id) " \
                "WHERE f.uniquename IN ('%s')"
        dc = self.query_fb(query % "','".join(fbids))
        results = []
        old_key = ''
        out = {}
        for d in dc:
            key = d['fbid']
            if not (key == old_key):
                if out: results.append(out)
                out = {}
                out['fbid'] = d['fbid']
                out['synonyms'] = set()
            if d['stype'] == 'symbol' and d['is_current']:
                out['label'] = clean_sgml_tags(d['unicode_name'])
            else:
                out['synonyms'].add(clean_sgml_tags(d['ascii_name']))
                out['synonyms'].add(clean_sgml_tags(d['unicode_name']))
            old_key = key
        return results

    def add_features(self, fbids):
        """Takes a list of fbids, generates a csv and uses this to merge feature nodes,
        adding a unicode label and a list of synonyms"""
        names = self.name_synonym_lookup(fbids)
        proc_names = [{'fbid': r['fbid'], 'label': r['label'],
                       'synonyms': '|'.join(r['synonyms'])}
                      for r in names]  # bit ugly...
        statement = "MERGE (n:Feature { short_form : line.fbid } ) " \
                    "SET n.label = line.label , n.synonyms = split(line.synonyms, '|')"  # Need to set iri
        self.commit_via_csv(statement, proc_names)

    # Typing

    def grossType(self, fbids):
        query = "SELECT f.uniquename AS fbid, c.name as ftype " \
                "FROM feature f " \
                "JOIN cvterm c on f.type_id=c.cvterm_id " \
                "WHERE f.uniquename in ('%s')" % "','".join(fbids)
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['fbid'],
                            map_feature_type(fbid=d['fbid'],
                                             ftype=d['ftype'])))
        return results

    def addTypes2Neo(self, fbids, detail='gross'):
        """Classify FlyBase features identified by a list of fbids.
        Optionally choose detailed classification with detail = 'fine'.
        (This option is currently experimental)."""
        statements = []
        if detail == 'gross':
            types = self.grossType(fbids)
        elif detail == 'fine':
            types = self.fineType(fbids)
        else:
            raise ValueError('detail arg invalid %s' % detail)

        feature_classifications = [{'child': t[0], 'parent': t[1]} for t in types]
        statement = "MATCH (p:Class { short_form: line.parent })" \
                    ",(c:Feature { short_form: line.child }) " \
                    "MERGE (p)<-[:SUBCLASSOF]-(c)"
        self.commit_via_csv(statement, feature_classifications)

    def abberationType(self, abbs):
        """abbs = a list of abberation fbids
        Returns a list of (fbid, type) tuples where type is a SO ID"""
        # Super slow and broken!
        results = []
        abbs_proc = []  # For tracking processed abbs
        query = "SELECT f.uniquename AS fbid, db.name AS db," \
                "dbx.accession AS acc " \
                "FROM feature f " \
                "JOIN cvterm gross_type ON gross_type.cvterm_id=f.type_id " \
                "JOIN feature_cvterm fc ON fc.feature_id = f.feature_id " \
                "JOIN cvterm fine_type ON fine_type.cvterm_id = fc.cvterm_id " \
                "JOIN feature_cvtermprop fctp ON fctp.feature_cvterm_id = fc.feature_cvterm_id " \
                "JOIN cvterm meta ON meta.cvterm_id = fctp.type_id " \
                "JOIN cvterm gtyp ON gtyp.cvterm_id = f.type_id " \
                "JOIN dbxref dbx ON fine_type.dbxref_id = dbx.dbxref_id " \
                "JOIN db ON dbx.db_id = db.db_id " \
                "WHERE gross_type.name = 'chromosome_structure_variation' -- double checks input gross type" \
                "AND  meta.name = 'wt_class'" \
                "AND f.uniquename in (%s)" % ("'" + "'.'".join(abbs))
        dc = self.query_fb(query)
        for d in dc:
            results.append((d['fbid'], d['db'] + '_' + d['acc']))
            abbs_proc.append(d['fbid'])
        [results.append((a, 'SO_0000110')) for a in abbs if
         a not in abbs_proc]  # Defaulting to generic feature id not abb
        return results

    def fineType(self, fbids):
        gt = self.grossType()
        abbs_list = []
        results = []
        for g in gt:
            if g[1] == '':
                abbs_list.append(g[0])
            else:
                results.append(g)
            results.extend(self.abberationType(abbs_list))

    def _get_objs(self, subject_ids, chado_rel, out_rel, o_idp):
        query_template = "SELECT s.uniquename AS subj, o.uniquename AS obj FROM feature s " \
                         "JOIN feature_relationship fr ON fr.subject_id=s.feature_id " \
                         "JOIN cvterm r ON fr.type_id=r.cvterm_id " \
                         "JOIN feature o ON fr.object_id=o.feature_id " \
                         "WHERE s.uniquename IN ('%s') " \
                         "AND r.name = '%s' " \
                         "AND o.uniquename like '%s'"
        query = query_template % ("','".join(subject_ids), chado_rel, o_idp + '%')
        dc = self.query_fb(query)
        results = []
        for d in dc:
            results.append((d['subj'], out_rel, d['obj']))
        return results

    def allele2Gene(self, subject_ids):
        """Takes a list of allele IDs, returns a list of triples as python tuples:
         (allele rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='alleleof', out_rel='is_allele_of', o_idp='FBgn')

    # gp - transgene R associated_with Type object by uniquename FBgn
    def gp2Transgene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel transgene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='fu', o_idp='FBti|FBtp')

    # gp - gene associated_with Type object by uniquename FBgn
    def gp2Gene(self, subject_ids):
        """Takes a list of gene product IDs, returns a list of triples as python tuples:
         (gene_product rel gene) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='expressed_by', o_idp='FBgn')

    # transgene - allele  R associated_with Type object by uniquename FBal
    def transgene2allele(self, subject_ids):
        """Takes a list of transgene IDs, returns a list of triples as python tuples:
         (transgene rel allele) where rel is appropriate for addition to prod."""
        return self._get_objs(subject_ids, chado_rel='associated_with', out_rel='fu', o_idp='FBal')

    def add_feature_relations(self, triples, assume_subject=True):
        if not assume_subject:
            subjects = [t[0] for t in triples]
            self.add_features(subjects)
            self.addTypes2Neo(subjects)
        objects = [t[2] for t in triples]
        self.add_features(objects)
        self.addTypes2Neo(objects)
        statements = []
        for t in triples:
            statements.append(
                "MATCH (s:Feature { short_form: '%s'}), (o:Feature { short_form: '%s'}) " \
                "MERGE (s)-[r:%s]->(o)" % (t[0], t[2], t[1])
            )
        self.nc.commit_list_in_chunks(statements)

    def generate_expression_pattern(self):
        return