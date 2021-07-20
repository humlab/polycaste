using ZipFile, Pkg

"""

zarchive = ZipFile.Reader("myarchive.zip")

for f in zarchive.files
    println(f.name)
    fullFilePath = joinpath(rootDir,dataFolder,f.name)
    if endswith(f.name,"/")
        mkdir(fullFilePath)
    else
        out =  open(fullFilePath,"w")
        write(out,read(f,String))
        close(out)
    end
end

close(zarchive)

"""
is_folder(str::AbstractString) = str != "" && occursin(str[end], "/\\")

function unzip(filename, target_folder)
    source_path = isabspath(filename) ? filename : joinpath(pwd(), filename)
    isdir(target_folder) || mkdir(target_folder)
    zipfile = ZipFile.Reader(source_path)
    for file in zipfile.files
        target_filename = joinpath(target_folder, file.name)
        if is_folder(file.name)
            mkdir(target_filename) && continue
        end
        write(target_filename, read(file))
    end
    close(zipfile)
end


isinstalled(package::String) = any(x -> x.name == package && x.is_direct_dep, values(Pkg.dependencies()))

function install_package(packages...)
    for package in packages
        if !isinstalled(package)
            Pkg.add(package)
        end
        @eval using $(Symbol(package))
    end
end
