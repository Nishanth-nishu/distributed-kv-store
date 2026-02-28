# CMake generated Testfile for 
# Source directory: /scratch/nishanth.r/proj/tests
# Build directory: /scratch/nishanth.r/proj/build/tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(test_consistent_hash "/scratch/nishanth.r/proj/build/tests/test_consistent_hash")
set_tests_properties(test_consistent_hash PROPERTIES  _BACKTRACE_TRIPLES "/scratch/nishanth.r/proj/tests/CMakeLists.txt;19;add_test;/scratch/nishanth.r/proj/tests/CMakeLists.txt;22;add_kvtest;/scratch/nishanth.r/proj/tests/CMakeLists.txt;0;")
add_test(test_storage_engine "/scratch/nishanth.r/proj/build/tests/test_storage_engine")
set_tests_properties(test_storage_engine PROPERTIES  _BACKTRACE_TRIPLES "/scratch/nishanth.r/proj/tests/CMakeLists.txt;19;add_test;/scratch/nishanth.r/proj/tests/CMakeLists.txt;23;add_kvtest;/scratch/nishanth.r/proj/tests/CMakeLists.txt;0;")
add_test(test_wal "/scratch/nishanth.r/proj/build/tests/test_wal")
set_tests_properties(test_wal PROPERTIES  _BACKTRACE_TRIPLES "/scratch/nishanth.r/proj/tests/CMakeLists.txt;19;add_test;/scratch/nishanth.r/proj/tests/CMakeLists.txt;24;add_kvtest;/scratch/nishanth.r/proj/tests/CMakeLists.txt;0;")
add_test(test_quorum "/scratch/nishanth.r/proj/build/tests/test_quorum")
set_tests_properties(test_quorum PROPERTIES  _BACKTRACE_TRIPLES "/scratch/nishanth.r/proj/tests/CMakeLists.txt;19;add_test;/scratch/nishanth.r/proj/tests/CMakeLists.txt;25;add_kvtest;/scratch/nishanth.r/proj/tests/CMakeLists.txt;0;")
subdirs("../_deps/googletest-build")
