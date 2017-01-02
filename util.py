
# ----------- 
# NOTE: this approach is specific to this particular competition
#
# The MAP metric here just boils down to knowing where you ended up ranking the actual ad that was clicked, relative
# to the other ads in its display context.
# ----------- 


# Now this is a quicker way to evaluate your score without needing to groupby or use the default MAP@ functions.
#
# After sorting each context in order of decreasing predicted probability, and giving each row in the dataset a sequential 
# index, then the delta between the index of the clicked ad, and the index of the first ad in the context, will give you
# the relative rank of the clicked ad within each context
import time
import numpy as np
def fast_mapk(validation_set):
#input: validation set with display_id,likelihood,clicked    
    validation_set.sort_values(['display_id', 'likelihood'], inplace=True, ascending=[True, False] )
    t=time.time()
    validation_set["seq"] = np.arange(validation_set.shape[0])
    print time.time()-t

    t=time.time()
    Y_seq           = validation_set[ validation_set.clicked == 1 ].seq.values
    print time.time()-t

    t=time.time()
    Y_first         = validation_set[['display_id', 'seq']].drop_duplicates(subset='display_id', keep='first').seq.values
    print time.time()-t

    t=time.time()
    Y_ranks         = Y_seq - Y_first
    print time.time()-t

    t=time.time()
    # At this point, some simplification of the MAP function given what we know about this competition gives us this quick calc
    score           = np.mean( 1.0 / (1.0 + Y_ranks) )
    print time.time()-t

    print("MAP: %.12f" % score)


def slow_mapk(val):
    t=time.time()

    val.sort_values(['display_id', 'likelihood'], inplace=True, ascending=[True, False] )

    print time.time()-t
    t=time.time()
    # Slower way

    from ml_metrics import mapk

    Y_ads = val[ val.clicked == 1 ].ad_id.values.reshape(-1,1)

    print time.time()-t
    t=time.time()
    P_ads = val.groupby(by='display_id', sort=False).ad_id.apply( lambda x: x.values ).values

    print time.time()-t
    t=time.time()
    score = mapk( Y_ads, P_ads, 12 )
    print time.time()-t
    t=time.time()
    print("MAP: %.12f" % score)
