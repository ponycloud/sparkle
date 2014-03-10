# Testing Placement

Every test scenario consists of multiple steps that alter the model and verify
resulting placement.  It is advisable that tests end up clearing the model in
order to verify generic withdrawal along the corresponding bestowment.

As for targeted withdrawal testing, it might be interesting to verify that
scraping large structures such as a host with it's children and deconstructing
them step by step both won't leave anything behind.

For the purpose of testing the bestowment to *any host in a set* is to be
treated as to *all hosts in the set*.  This may cause problems with detection
in bestow-to-many vs. bestow-to-any situations.

