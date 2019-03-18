cp *.cc *.hh Makefile ../../fastpr-v1.0/
cp -r Jerasure/ ../../fastpr-v1.0/
cp -r Util/ ../../fastpr-v1.0/ 
cp -r metadata/ ../../fastpr-v1.0/ 
cp readme.md ../../fastpr-v1.0/ 
rm ../../fastpr-v1.0.tar.gz
tar -czvf ../../fastpr-v1.0.tar.gz ../../fastpr-v1.0/
