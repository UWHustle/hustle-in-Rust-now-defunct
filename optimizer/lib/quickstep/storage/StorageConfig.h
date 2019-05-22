/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 **/

/* #undef QUICKSTEP_HAVE_BITWEAVING */
#define QUICKSTEP_HAVE_FILE_MANAGER_POSIX
/* #undef QUICKSTEP_HAVE_FILE_MANAGER_WINDOWS */
/* #undef QUICKSTEP_HAVE_FILE_MANAGER_HDFS */
#define QUICKSTEP_REBUILD_INDEX_ON_UPDATE_OVERFLOW
/* #undef QUICKSTEP_HAVE_MMAP_LINUX_HUGETLB */
/* #undef QUICKSTEP_HAVE_MMAP_BSD_SUPERPAGE */
#define QUICKSTEP_HAVE_MMAP_PLAIN
/* #undef QUICKSTEP_HAVE_LIBNUMA */
