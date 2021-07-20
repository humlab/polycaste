using Test, Polycaste, ZipFile, CSV, DataFrames
using Polycaste:extract_lemma
# Arrow.write(io, CSV.File(file))

# corpus = TaggedDocumentCorpus("data/riksdagens-protokoll.1970.sparv4.csv.zip")

@testset "Extract lemma" begin

    @test extract_lemma("") === missing
    @test extract_lemma("|") === missing
    @test extract_lemma("apa") == "apa"
    @test extract_lemma("|apa") == "apa"
    @test extract_lemma("|apa|") == "apa"
    @test extract_lemma("apa|") == "apa"
    @test extract_lemma("apa|hej") == "apa"
    @test extract_lemma("apa:15|hej") == "apa"
    @test extract_lemma("|apa:|hej") == "apa"
    @test extract_lemma(" |apa:|hej") == "apa"
    @test extract_lemma("aéb𐅍cd|") == "aéb𐅍cd"

end
