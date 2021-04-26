from nlcpy import veo
import os
import sys

nodeid = os.environ.get("VE_NODE_NUMBER", 0)
bld = veo.VeBuild()
bld.set_build_dir("_ve_build")
bld.set_c_src("_sum", r"""

double avg(double *a, int n)
{
    int i;
    if ( n == 0 ) {
        return 0;
    }
    double sum = 0;
    for (i = 0; i < n; i++) {
        sum += a[i] / n;
    }
    return sum;
}""")

ve_so_name = bld.build_so()

proc = veo.VeoAlloc().proc
ctxt = proc.open_context()
lib = proc.load_library((os.getcwd() + "/" + ve_so_name).encode("UTF-8"))
lib.avg.args_type(b"double*", "int")
lib.avg.ret_type("double")
np = veo.np

np_numbers = np.array(numbers)
a_ve = proc.alloc_mem(len(numbers) * 8)
proc.write_mem(a_ve, np_numbers, len(numbers) * 8)
req = lib.avg(ctxt, a_ve, len(numbers))
avg = req.wait_result()

print(avg)

proc.free_mem(a_ve)
del proc
