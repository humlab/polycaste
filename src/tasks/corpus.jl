module ProcessCorpus

using ZipFile, CSV, DataFrames
using ProgressMeter
using Arrow
using Base.Threads
using DataStructures
using Logging
using SparseArrays

export process

strip_path_ext = x -> splitext(basename(x))[1]

mutable struct TaggedDocument
    filename::String
    document_id::Int
    document_name::String
    tagged_frame::DataFrame
end

mutable struct TaggedDocumentCorpus
    corpus_name::String
    documents::Vector{TaggedDocument}
end

function extract_lemma(lemmas::AbstractString)
    try
        lemmas === missing && return missing
        lemmas = strip(lemmas)
        lemmas == "" && return missing
        lemmas == "|" && return missing
        lemma = split(lemmas, '|', keepempty = false)[1]
        lemma = split(lemma, ':')[1]
        return replace(lemma, " " => "_")
    catch e
        print("error: $e")
    end
end

function TaggedDocument(filename::AbstractString, document_id::Int, tagged_frame::DataFrame)
    doc = TaggedDocument(basename(filename), document_id, strip_path_ext(filename), tagged_frame)
    return doc
end

function TaggedDocumentCorpus(source::AbstractString)

    documents = TaggedDocument[]

    reader = ZipFile.Reader(source)
    for (i, file) in enumerate(reader.files)
        document = TaggedDocument(filename, i, tagged_frame(file))
        push!(documents, document)
    end
    close(reader)

    corpus::TaggedDocumentCorpus = TaggedDocumentCorpus(corpus_name, documents)
    return corpus

end


function readz(file, lck::ReentrantLock)
    data = nothing
    try
        lock(lck)
        data = read(file)
    catch e
        println("failed: $(file.name) $(e)")
    finally
        unlock(lck)
    end
    return data
end

Base.@kwdef struct ReadOpts
    delim::Char = '\t'
    decimal::Char = '.'
    token::AbstractString = "token"
    lemma::AbstractString = "baseform"
    header::Int = 1
    datarow::Int = 3
end

function tagged_frame()
    DataFrame(token=[], pos=[], baseform=[])
end

function tagged_frame(file, read_opts::ReadOpts, read_lock::ReentrantLock = nothing)
    try
        data = read_lock !== nothing ? readz(file, read_lock) : read(file)
        data === nothing && return tagged_frame()
        tf =
            CSV.File(
                data,
                delim = read_opts.delim,
                quotechar = 0x0,
                decimal = read_opts.decimal,
                header = read_opts.header,
                datarow = read_opts.datarow,
            ) |> DataFrame
        tf.baseform = extract_lemma.(tf[!, read_opts.lemma])
        size = nrow(tf)

        tf = dropmissing!(tf)

        if nrow(tf) == 0
            @warn "file is empty" file.name
        elseif nrow(tf) < size
            @warn "warning: file $(file.name), $(size-nrow(tf)) rows with missing value8s) dropped"
        end

        tf.baseform = lowercase.(tf[!, read_opts.lemma])

        return tf
    catch e
        @error "failed " file.name e
        return tagged_frame()
    end
end

function tagged_frame(filename::AbstractString)
    try
        return Arrow.Table(filename) |> DataFrame
    catch e
        @warn "failed to read: $(filename)"
        return tagged_frame()
    end
end

function tagged_document(filename::String, document_id::Int, tagged_frame::DataFrame)
    return TaggedDocument(filename, document_id, strip_path_ext(filename), tagged_frame)
end

function tagged_documents(filename::AbstractString, read_opts::ReadOpts)
    global zipfile = ZipFile.Reader(filename)
    return (
        TaggedDocument(file.name, filename, tagged_frame(file, read_opts)) for (i, file) in enumerate(zipfile.files)
    )
end

Base.@kwdef mutable struct TokenCounts
    target::String = ""
    lock = ReentrantLock()
    thread_counts = Dict{Int,Dict{String,Int}}()
    corpus_counts = Dict{String,Int}()
    document_counts = Dict{Int,Accumulator{String,Int}}()
    vocabulary_frame::Union{DataFrame,Nothing} = nothing
    dtm::Union{SparseMatrixCSC{Int, Int}, Nothing} = nothing
end

function store(counts::TokenCounts, target_folder::AbstractString)
    token2id_filename = joinpath(target_folder, "token2id.$(counts.target).arrow")
    Arrow.write(token2id_filename, counts.vocabulary_frame)
end

function update!(counts::TokenCounts, document::TaggedDocument)
    """Document update"""
    token_counts::Accumulator{String,Int} = counter(document.tagged_frame[!, counts.target])

    try
        lock(counts.lock)
        counts.document_counts[document.document_id] = token_counts
    finally
        unlock(counts.lock)
    end

    thread_counts = get!(Dict{String,Accumulator{String,Int}}, counts.thread_counts, threadid())
    mergewith!(+, thread_counts, token_counts)

end

function update!(counts::TokenCounts)
    """Corpus update"""

    merge!(counts.corpus_counts, values(counts.thread_counts)...)

    counts.vocabulary_frame = DataFrame(
        token = [x for x in keys(counts.corpus_counts)],
        token_id = [x for x = 0:(length(counts.corpus_counts)-1)],
        tf = [x for x in values(counts.corpus_counts)],
    )

    counts.dtm = document_term_matrix(counts)

end

function document_index(source::AbstractString)
    global reader = ZipFile.Reader(source)
    df = document_index(reader)
    close(reader)
    return df
end

function document_index(reader::ZipFile.Reader, extension::AbstractString = "csv")
    df = DataFrame((
        (document_id = i - 1, filename = basename(file.name), document_name = strip_path_ext(file.name)) for
        (i, file) in enumerate(reader.files) if endswith(file.name, extension)
    ))
    return df
end


function document_term_matrix(data::TokenCounts)
    token2id = Dict(Pair.(data.vocabulary_frame.token, data.vocabulary_frame.token_id))
    return document_term_matrix(data.document_counts, token2id)
end

function document_term_matrix(document_counts::Dict{Int, Accumulator{String, Int}}, token2id::Dict{String, Int})
    """Returns a DTM for given counts

    h5open("data.h5", "w") do file
        write(file, "A", A)
    end

    or

    @save("sparsematrix.jld",a)

    c = h5open("mydata.h5", "r") do file
        read(file, "A")
    end

    ### Load in Python

    import h5py
    from scipy.sparse import csc_matrix

    filename="sparsematrix.jld"
    f = h5py.File(filename, 'r')
    data= f["a"][()]

    column_ptr=f[data[2]][:]-1 ## correct indexing from julia (starts at 1)
    indices=f[data[3]][:]-1 ## correct indexing
    values =f[data[4]][:]
    csc_matrix((values,indices,column_ptr), shape=(data[0],data[1])).toarray()

    f.close()

    """

    rows = Array{Int}(undef, 0)
    columns = Array{Int}(undef, 0)
    frequencies = Array{Int}(undef, 0)
    d = 0
    for (document_id, counts) in document_counts  # BASE 0?
        d = max(d, document_id)
        for (t, v) in counts
            push!(rows, document_id)
            push!(columns, token2id[t])
            push!(frequencies, v)
        end
    end

    n = length(token2id)

    return length(rows) > 0 ? sparse(rows, columns, frequencies, d, n) : spzeros(Int, d, n)

end


function process(filename::AbstractString, target_folder::AbstractString, force::Bool = false)

    global reader = ZipFile.Reader(filename)

    # if force
    #     rm(target_folder, recursive = true, force = force)
    # end

    if !isdir(target_folder)
        mkpath(target_folder)
    end

    read_lock = ReentrantLock()
    read_opts = ReadOpts()
    index = document_index(reader)
    name2id = Dict(Pair.(index.document_name, index.document_id))

    counters = [TokenCounts(target = read_opts.token), TokenCounts(target = read_opts.lemma)]
    files = filter(f -> endswith(f.name, "csv"), [f for f in reader.files])

    p = Progress(length(files))
    i = Atomic{Int}(1)

    @inbounds @threads for file in files

        atomic_add!(i, 1)

        document_name = strip_path_ext(file.name)
        document_id = name2id[document_name]
        target_filename = joinpath(target_folder, "$(strip_path_ext(file.name)).arrow")

        if force || !isfile(target_filename)
            tf = tagged_frame(file, read_opts, read_lock)
        else
            tf = tagged_frame(target_filename)
        end

        if tf === nothing
            @warn "failed to read: $(file.name)"
            continue
        end

        document = tagged_document(target_filename, document_id, tf)

        if force || !isfile(target_filename)
            Arrow.write(target_filename, document.tagged_frame)
        end

        map(x -> update!(x, document), counters)

        next!(p)
    end

    close(reader)

    map(update!, counters)
    map(x -> store(x, target_folder), counters)

    Arrow.write(joinpath(target_folder, "document_index.arrow"), index)

end

end


# if abspath(PROGRAM_FILE) == @__FILE__
    #filename = "data/riksdagens-protokoll.1920-2019.9files.sparv4.csv.zip"
    filename = "data/riksdagens-protokoll.1970.sparv4.csv.zip"
    # filename = "/data/westac/data/riksdagens-protokoll.1920-2019.sparv4.csv.zip"
    ProcessCorpus.process(filename, "/data/westac/julia", false)

    # Profile.clear()
    # @profile process(filename, "/data/westac/julia", true)
    # open(Profile.print, "profile.log", "w")
# end