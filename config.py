### DEPENDENCY TYPES FOR EXTENDED NGRAMS ###

# jump over prepositions and take the prep_obj instead
ext_zero=u"".split() ## we don't have these in Finnish

# 'multiword expressions', include functional dependencies if head token is included (UD types: cc adpos case goeswith mwe cc:preconj preconj)
ext_inc=u"".split() ## adpos is always included because it's the Finnish version of prep

#other functional dependencies (UD types: det ps complm prt neg aux auxpass mark compound:prt punct)
ext_special=u"".split()

# coordination types (UD types: conj)
coords=u"".split()

# coordinating conjunctions (UD types: cc, cc:preconj, preconj)
coord_conjs=u"".split()
