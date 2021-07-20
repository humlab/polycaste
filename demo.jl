##.
msg = "Hello World"
println(msg)
##.
function plus_two(x)
    #perform some operations
    return x + 2
end

a = [1,2,3,4,5]
print("HEJ")

try
    append!(a)
catch
    print("Errow")
end

print(a[2:3])

##.
function absolute(x)
    if x >= 0
        return x
    else
        return -x
    end
end

# DataFrames: https://dataframes.juliadata.org/stable/

##
#import Pkg;
# Pkg.add("DataFrames")

using DataFrames
using CSV
using Arrow

table = Arrow.Table("data/prot_1970__ak__1.feather")

print(length(table.token))

##.

using ZipFile, CSV, DataFrames

z = ZipFile.Reader("/data/westac/data/riksdagens-protokoll.1920-2019.sparv4.csv.zip")
idx = findfirst(x -> x.name == "prot_201112__46.csv", z.files)
# prot_201112__46.csv

df = CSV.File(read(z.files[idx])) |> DataFrame;
filter(x -> x.pos === missing, df) |> show
close(z)

##


# identify the right file in zip
#a_file_in_zip = filter(x->x.name == "prot_1970__ak__1.csv", z.files)[1]
#table = CSV.File(read(a_file_in_zip)) |> DataFrame
function process_files(s::String)

    for file in enumerate(z.files)

        #name = file[2].name
        #print("$(file[2].name)\n")

        table = CSV.File(read(file[2])) |> DataFrame;

    end
end
function process_files(i::Int64)

    for file in enumerate(z.files)

        #name = file[2].name
        #print("$(file[2].name)\n")

        table = CSV.File(read(file[2])) |> DataFrame;

    end
end

process_files(2);

### Naming


failed_arrow_reads = [
    "prot_201112__46.csv",
    "prot_198586__90.csv",
    "prot_200304__4.csv",
    "prot_198182__94.csv",
    "prot_1974__125.csv",
    "prot_200102__67.csv"
]

missing_pos = [
    "prot_201415__112.csv",
    "prot_201415__20.csv",
    "prot_201415__36.csv",
    "prot_201415__8.csv",
    "prot_201415__91.csv",
    "prot_201415__93.csv",
    "prot_201516__115.csv",
    "prot_201516__129.csv",
    "prot_201516__33.csv",
    "prot_201516__51.csv",
    "prot_201516__79.csv",
    "prot_201617__108.csv",
    "prot_201617__119.csv",
    "prot_201617__34.csv",
    "prot_201617__71.csv",
    "prot_201617__77.csv",
    "prot_201617__96.csv",
    "prot_201718__134.csv",
    "prot_201718__22.csv",
    "prot_201718__39.csv",
    "prot_201718__44.csv",
    "prot_201718__47.csv",
    "prot_201718__91.csv",
    "prot_201718__95.csv",
    "prot_201819__13.csv",
    "prot_201819__80.csv",
    "prot_201819__88.csv",
    "prot_201819__97.csv",
    "prot_201920__19.csv",
    "prot_201920__33.csv",
    "prot_201920__44.csv"
]