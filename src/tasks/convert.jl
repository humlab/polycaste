using Test, Polycaste, ZipFile, CSV, DataFrames
# Arrow.write(io, CSV.File(file))




function load_corpus(filename::AbstractString)

    global zipfile = ZipFile.Reader(filename);
    for (i, file) in enumerate(zipfile.files)
        data = read(file, String)
        # print("File $i: $(file.name) $(data[1:100])\n")
        println("File $i: $(file.name)")

        # tf = tagged_frame(read(file))
        tf = CSV.File(file, DataFrame; delim='\t', quotechar=0x0, decimal='.', header=1, datarow=3)
        #tf = CSV.File(data, delim='\t', quotechar=0x0, decimal='.', header=1, datarow=3) |> DataFrame;

        # document = TaggedDocument(filename, filename, tagged_frame)
        # push!(documents, document)
    end
end

